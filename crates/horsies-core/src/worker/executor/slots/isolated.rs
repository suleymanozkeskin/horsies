//! One isolated slot: boot a child, serve tasks on it one at a time, hold
//! the deadline of each, recycle or replace the child (Python
//! `core/worker/slots.py`, `_run_generation`, `_read_answer`,
//! `_end_after_return`; `core/worker/dispatch.py`, the deadline kill).
//!
//! Rules of one task:
//! - The deadline starts when the run frame is sent. At the deadline the
//!   child is killed with its session and the end is `TimedOut` once its
//!   exit is seen; a result after the deadline is never read.
//! - A body that returned with work left on its loop keeps what it
//!   returned: the deadline no longer applies, and every later end of the
//!   child whose exit is seen writes what the body returned.
//! - A child lost with its exit seen takes the crash path.
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
use super::dispatch::{AttemptEnd, NotRunCause, TimeoutCancel};
use super::pool::{Answer, ExitNotSeen, MemoryLimit, PoolShared, QueuedRun, TaskLimit};
use super::route::IsolatedDeadline;
use super::seat_runs::SHARED_BACKSTOP_MARGIN;
use crate::broker::attempt_writes::RetryPlacement;
use crate::broker::child_writes::{longer, FINALIZING_MARKER_END};
use crate::db::CANCEL_REQUEST_BOUND;
use crate::worker::executor::protocol::{
    BodyEnd, ChildFrame, FailureDetail, ParentFrame, ProcessId, RssReading, RunEnd, RunFrame,
    TaskTimeoutMs, ISOLATED_LEFTOVER_GRACE, STUCK_REPORT_DELAY,
};
use crate::worker::executor::supervisor::{
    ChildChannel, ChildExit, ChildLost, ExitState, FrameWait, LossCause, MailboxBound, SendFailure,
};
use crate::worker::runner_beats::{DispatchRunner, HeldChild, HeldDispatch, RegistryClosed};

/// The longest wait of the finalizing marker of a Python isolated child
/// after a body returned with work left, for a pooled connection (Python
/// `LEFT_WORK_MARKER_CONNECTION_WAIT_S`).
pub const LEFT_WORK_MARKER_CONNECTION_WAIT: Duration = Duration::from_secs(5);

/// The longest time of the statement of that marker once it holds its
/// connection (Python `LEFT_WORK_MARKER_STATEMENTS_S`).
pub const LEFT_WORK_MARKER_STATEMENTS: Duration = Duration::from_secs(5);

/// The longest time of that marker: the wait for a connection, the
/// statement, and the cancel request of a statement past its bound (Python
/// `LEFT_WORK_MARKER_BOUND_S`, with `CANCEL_REQUEST_TIMEOUT_S`).
pub const LEFT_WORK_MARKER_END: Duration = LEFT_WORK_MARKER_CONNECTION_WAIT
    .saturating_add(LEFT_WORK_MARKER_STATEMENTS)
    .saturating_add(CANCEL_REQUEST_BOUND);

/// After a body returned with work left on its loop, the longest time until
/// a Python isolated child reports how that work ended: the grace
/// ([`ISOLATED_LEFTOVER_GRACE`], 1 s), the cancel rounds
/// ([`STUCK_REPORT_DELAY`], 0.3 s) and its marker
/// ([`LEFT_WORK_MARKER_END`], 15 s): 16.3 s.
pub const PYTHON_CHILD_LEFTOVER_END: Duration = ISOLATED_LEFTOVER_GRACE
    .saturating_add(STUCK_REPORT_DELAY)
    .saturating_add(LEFT_WORK_MARKER_END);

/// After a body returned with work left on its loop, the longest time until
/// a core child reports how that work ended: the grace, the cancel rounds
/// and its finalizing marker ([`FINALIZING_MARKER_END`], 20 s): 21.3 s.
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
/// a child's own refusal. Python `ISOLATED_LEFTOVER_BACKSTOP_S` (26.3 s)
/// counts only the Python child, the one kind that a Python parent runs.
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
    /// The child is lost or killed. The task finished when its body
    /// returned before.
    ChildGone(TasksFinished),
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

/// The answer of the task on the child, kept by the slot outside the code
/// that runs the task, so that a defect in that code still answers it
/// (Python `SlotHandle.finish`, `loss_outcome`).
struct TaskAnswer {
    answer: Option<Answer>,
    returned: IsolatedReturn,
}

impl TaskAnswer {
    fn new(answer: Answer) -> Self {
        Self {
            answer: Some(answer),
            returned: IsolatedReturn::NotSeen,
        }
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
    /// exit of its child, lost by `lost`, is seen.
    fn open_at_loss(&mut self, lost: &ChildLost) -> Option<(Answer, AttemptEnd)> {
        let answer = self.answer.take()?;
        let seen_end = match &self.returned {
            IsolatedReturn::Seen(returned) => returned.clone(),
            IsolatedReturn::NotSeen => AttemptEnd::Crashed {
                reason: FailureDetail::cut(&lost.to_string()),
            },
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

/// The slot task: boot a child, serve it, replace it, until the pools stop
/// or fail. `index` is the slot's index on the board. A defect costs the
/// child and its task; [`SLOT_DEFECT_LIMIT`] defects with no finished task
/// between them fail the pools.
///
/// [`SLOT_DEFECT_LIMIT`]: super::defects::SLOT_DEFECT_LIMIT
pub(super) async fn run_slot(shared: Arc<PoolShared>, index: usize) {
    let mut defects = SlotDefects::default();
    loop {
        let served = CatchPanic::new(boot_and_serve(&shared, index, &mut defects)).await;
        let defect = match served {
            Ok(Served::PoolEnded) => return,
            Ok(Served::Generation(GenerationEnd::Ended)) => None,
            Ok(Served::Generation(GenerationEnd::Defect(defect))) => Some(defect),
            Err(payload) => {
                // The child of the generation was dropped with it; its
                // channel kills it on drop.
                shared.board.set(index, SlotPhase::Retiring);
                Some(SlotDefect::panicked(payload.as_ref()))
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
            false => shared.board.set(index, SlotPhase::Booting),
        }
    }
}

/// Boot a child for the slot and serve it.
async fn boot_and_serve(shared: &PoolShared, index: usize, defects: &mut SlotDefects) -> Served {
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
            Served::Generation(serve_generation(shared, index, channel, defects).await)
        }
        Booted::PoolStopped | Booted::PoolFailed => Served::PoolEnded,
    }
}

/// Serve tasks on one child until it is lost, retires, or the pool stops.
/// A panic while a task runs is a defect: the child is killed and the task
/// answered here.
async fn serve_generation(
    shared: &PoolShared,
    index: usize,
    mut channel: ChildChannel,
    defects: &mut SlotDefects,
) -> GenerationEnd {
    let mut tasks_done: u64 = 0;
    let mut notice = ReadyNotice::AtFirstWait;
    loop {
        let Some(run) = next_run(shared, &channel, notice).await else {
            break;
        };
        notice = ReadyNotice::Quiet;
        tasks_done = tasks_done.saturating_add(1);
        let QueuedRun {
            frame,
            deadline,
            answer,
        } = run;
        let mut answer = TaskAnswer::new(answer);
        let ran =
            CatchPanic::new(run_one(shared, &mut channel, frame, deadline, &mut answer)).await;
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
                if let Some(reason) = recycle_reason(shared, tasks_done, rss) {
                    tracing::debug!(
                        slot = index,
                        pid = channel.pid().get(),
                        reason,
                        "retiring a child"
                    );
                    break;
                }
            }
            AfterRun::Retire => {
                defects.task_finished();
                break;
            }
            AfterRun::ChildGone(finished) => {
                defects.note(finished);
                shared.board.set(index, SlotPhase::Retiring);
                let _ = channel.dispose_lost().await;
                return GenerationEnd::Ended;
            }
            AfterRun::Defect(defect) => {
                shared.board.set(index, SlotPhase::Retiring);
                let _ = channel.dispose_lost().await;
                return GenerationEnd::Defect(defect);
            }
        }
    }
    shared.board.set(index, SlotPhase::Retiring);
    match channel.has_exited() || channel.channel_closed() {
        true => {
            let _ = channel.dispose_lost().await;
        }
        false => {
            let _ = channel.retire().await;
        }
    }
    GenerationEnd::Ended
}

/// Why a child that served a task retires, if it does.
fn recycle_reason(shared: &PoolShared, tasks_done: u64, rss: RssReading) -> Option<&'static str> {
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
        (true, _) => Some("task count"),
        (false, true) => Some("memory limit"),
        (false, false) => None,
    }
}

/// Wait for work. `None`: this child must not take any. While it waits,
/// the slot counts as idle in the free slots of its pool. With
/// [`ReadyNotice::AtFirstWait`], the first time the slot waits with no task
/// queued it notes the ready child on the board, after it counts as idle:
/// the claim pass that the note wakes counts this slot as free.
async fn next_run(
    shared: &PoolShared,
    channel: &ChildChannel,
    mut notice: ReadyNotice,
) -> Option<QueuedRun> {
    let mut exit = channel.exit_watch();
    let mut idle = shared.idle_slot();
    loop {
        let cannot_serve =
            shared.board.not_serving() || channel.has_exited() || channel.channel_closed();
        if cannot_serve {
            return None;
        }
        if let Some(run) = idle.take() {
            return Some(run);
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
                    return None;
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

/// Run one task on the child and answer it.
async fn run_one(
    shared: &PoolShared,
    channel: &mut ChildChannel,
    frame: RunFrame,
    deadline: IsolatedDeadline,
    answer: &mut TaskAnswer,
) -> AfterRun {
    let task_id = frame.task_id;
    let runner = DispatchRunner::Child(HeldChild {
        id: channel.id(),
        pid: channel.pid(),
        spawner: shared.spawner.clone(),
        kill_mark: channel.kill_mark(),
    });
    let held = match shared.live.hold(task_id, frame.claimed_at, runner) {
        Ok(held) => held,
        Err(RegistryClosed) => {
            // No beat would cover the task: it is not sent, and the child
            // is killed with the slot's lost-child path.
            tracing::error!(
                task_id = %task_id,
                pid = channel.pid().get(),
                "the runner heartbeats failed closed; the task is not sent",
            );
            answer.send(AttemptEnd::NotDispatched(
                NotRunCause::RunnerBeatsFailedClosed,
            ));
            return AfterRun::ChildGone(TasksFinished::NoTask);
        }
    };
    answer.child_took_it(channel.pid());
    let parent_frame = ParentFrame::Run(frame);
    let sent = channel.send(&parent_frame).await;
    // The input is not kept once the frame is written.
    drop(parent_frame);
    let timeout = match deadline {
        IsolatedDeadline::NoDeadline => None,
        IsolatedDeadline::After(timeout) => Some(timeout),
    };
    let first = match sent {
        Ok(()) => {
            let until =
                timeout.map(|timeout| Instant::now() + Duration::from_millis(timeout.get()));
            first_answer(shared, channel, until).await
        }
        Err(SendFailure::Encode(error)) => {
            // Nothing reached the child: the frame failed in the slot's
            // own path, which is a defect (Python `_serve_slot`).
            tracing::error!(
                task_id = %task_id,
                error = %error,
                "the run frame could not be encoded: {error}",
            );
            drop(held);
            let lost = defect_kill(channel).await;
            answer.send(end_of_loss(shared, channel, lost).await);
            return AfterRun::Defect(SlotDefect::frame_not_encoded(&error));
        }
        Err(failure @ (SendFailure::Write(_) | SendFailure::TimedOut)) => {
            tracing::warn!(task_id = %task_id, failure = %failure, "the run frame could not be sent");
            drop(held);
            let lost = lost_after_kill(channel).await;
            answer.send(end_of_loss(shared, channel, lost).await);
            return AfterRun::ChildGone(TasksFinished::NoTask);
        }
    };
    match first {
        FirstAnswer::Wait(FrameWait::Frame(ChildFrame::Result(result)))
            if result.task_id == task_id =>
        {
            drop(held);
            answer.send(AttemptEnd::Reported(result.end));
            AfterRun::Serve { rss: result.rss }
        }
        FirstAnswer::Wait(FrameWait::Frame(ChildFrame::BodyReturned(returned)))
            if returned.task_id == task_id =>
        {
            tracing::debug!(
                task_id = %task_id,
                left = returned.left.count().get(),
                "the body returned with work left on its loop; the deadline no longer applies",
            );
            end_after_return(shared, channel, task_id, returned.returned, held, answer).await
        }
        FirstAnswer::Wait(FrameWait::Frame(other)) => {
            tracing::error!(task_id = %task_id, frame = ?other, "an isolated child sent a frame out of turn");
            drop(held);
            let lost = channel.discard_violating_child().await;
            answer.send(end_of_loss(shared, channel, lost).await);
            AfterRun::ChildGone(TasksFinished::NoTask)
        }
        FirstAnswer::Wait(FrameWait::Lost(lost)) => {
            drop(held);
            answer.send(end_of_loss(shared, channel, lost).await);
            AfterRun::ChildGone(TasksFinished::NoTask)
        }
        FirstAnswer::Wait(FrameWait::Expired) => {
            drop(held);
            let timed_out = timeout_end(timeout);
            answer.send(end_after_kill(shared, channel, timed_out).await);
            AfterRun::ChildGone(TasksFinished::NoTask)
        }
        FirstAnswer::DrainDeadline => {
            drop(held);
            shared.drain_kills.note([task_id]);
            let crashed = AttemptEnd::Crashed {
                reason: FailureDetail::cut("the worker killed the child at the drain deadline"),
            };
            answer.send(end_after_kill(shared, channel, crashed).await);
            AfterRun::ChildGone(TasksFinished::NoTask)
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
        },
        None => unreachable!("a wait with no deadline cannot pass its deadline"),
    }
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
    let crashed = AttemptEnd::Crashed {
        reason: FailureDetail::cut(&lost.to_string()),
    };
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
    open.send(end_once_seen(shared, channel, lost, seen_end).await);
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
/// by [`ISOLATED_LEFTOVER_BACKSTOP`]. What the body returned is the end at
/// every end of the child whose exit is seen. The task finished: its body
/// returned.
async fn end_after_return(
    shared: &PoolShared,
    channel: &mut ChildChannel,
    task_id: uuid::Uuid,
    returned: BodyEnd,
    held: HeldDispatch,
    answer: &mut TaskAnswer,
) -> AfterRun {
    let returned_end = AttemptEnd::Reported(RunEnd::from(returned));
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
                task_id = %task_id,
                alive = stuck.still.count().get(),
                "the work a task left did not stop; killing its child",
            );
            drop(held);
            answer.send(end_after_kill(shared, channel, returned_end).await);
            AfterRun::ChildGone(TasksFinished::AtLeastOne)
        }
        FirstAnswer::Wait(FrameWait::Expired) => {
            drop(held);
            answer.send(end_after_kill(shared, channel, returned_end).await);
            AfterRun::ChildGone(TasksFinished::AtLeastOne)
        }
        FirstAnswer::DrainDeadline => {
            drop(held);
            // The child still holds the work the task left on its loop
            // (Python `_kill_at_deadline`: the handle still has its child).
            shared.drain_kills.note([task_id]);
            answer.send(end_after_kill(shared, channel, returned_end).await);
            AfterRun::ChildGone(TasksFinished::AtLeastOne)
        }
        FirstAnswer::Wait(FrameWait::Frame(other)) => {
            tracing::error!(task_id = %task_id, frame = ?other, "an isolated child sent a frame out of turn");
            drop(held);
            let lost = channel.discard_violating_child().await;
            answer.send(end_once_seen(shared, channel, lost, returned_end).await);
            AfterRun::ChildGone(TasksFinished::AtLeastOne)
        }
        FirstAnswer::Wait(FrameWait::Lost(lost)) => {
            drop(held);
            answer.send(end_once_seen(shared, channel, lost, returned_end).await);
            AfterRun::ChildGone(TasksFinished::AtLeastOne)
        }
    }
}

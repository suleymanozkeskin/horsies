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

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use super::board::SlotPhase;
use super::booting::{boot_child, lost_after_kill, BootSlot, Booted};
use super::dispatch::{AttemptEnd, NotRunCause};
use super::pool::{Answer, ExitNotSeen, MemoryLimit, PoolShared, QueuedRun, TaskLimit};
use super::route::IsolatedDeadline;
use crate::broker::attempt_writes::RetryPlacement;
use crate::worker::executor::protocol::{
    BodyEnd, ChildFrame, FailureDetail, ParentFrame, RssReading, RunEnd, TaskTimeoutMs,
};
use crate::worker::executor::supervisor::{
    ChildChannel, ChildExit, ChildLost, ExitState, FrameWait, MailboxBound, CHILD_KILL_WAIT,
};
use crate::worker::runner_beats::{DispatchRunner, HeldChild, RegistryClosed};

/// After a body returned with work left on its loop, the child reports how
/// that work ended within this time, or it is killed: 1 s of grace, 0.3 s
/// of cancel rounds, the 15 s bound of the finalizing marker, and 10 s of
/// margin (Python `ISOLATED_LEFTOVER_BACKSTOP_S`).
pub const ISOLATED_LEFTOVER_BACKSTOP: Duration = Duration::from_millis(26_300);

/// What the slot does after one task.
enum AfterRun {
    /// The child serves on; `rss` decides a recycle.
    Serve { rss: RssReading },
    /// The child stops after a clean end (left work was cancelled in it).
    Retire,
    /// The child is lost or killed.
    ChildGone,
}

/// What came first for a task in flight.
enum FirstAnswer {
    Wait(FrameWait),
    DrainDeadline,
}

/// The slot task: boot a child, serve it, replace it, until the pools stop
/// or fail. `index` is the slot's index on the board.
pub(super) async fn run_slot(shared: Arc<PoolShared>, index: usize) {
    loop {
        let boot = BootSlot {
            board: &shared.board,
            index,
            spawner: &shared.spawner,
            command: &shared.config.command,
            init: &shared.config.init,
            mailbox: MailboxBound::for_seats(NonZeroUsize::MIN),
        };
        match boot_child(&boot).await {
            Booted::Child(channel) => serve_generation(&shared, index, channel).await,
            Booted::PoolStopped | Booted::PoolFailed => return,
        }
        match shared.board.not_serving() {
            true => return,
            false => shared.board.set(index, SlotPhase::Booting),
        }
    }
}

/// Serve tasks on one child until it is lost, retires, or the pool stops.
async fn serve_generation(shared: &PoolShared, index: usize, mut channel: ChildChannel) {
    let mut tasks_done: u64 = 0;
    loop {
        let Some(run) = next_run(shared, &channel).await else {
            break;
        };
        tasks_done = tasks_done.saturating_add(1);
        match run_one(shared, &mut channel, run).await {
            AfterRun::Serve { rss } => {
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
            AfterRun::Retire => break,
            AfterRun::ChildGone => {
                shared.board.set(index, SlotPhase::Retiring);
                let _ = channel.dispose_lost().await;
                return;
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

/// Wait for work. `None`: this child must not take any.
async fn next_run(shared: &PoolShared, channel: &ChildChannel) -> Option<QueuedRun> {
    let mut exit = channel.exit_watch();
    loop {
        let cannot_serve =
            shared.board.not_serving() || channel.has_exited() || channel.channel_closed();
        if cannot_serve {
            return None;
        }
        if let Some(run) = shared.pop() {
            return Some(run);
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
async fn run_one(shared: &PoolShared, channel: &mut ChildChannel, run: QueuedRun) -> AfterRun {
    let QueuedRun {
        frame,
        deadline,
        mut answer,
    } = run;
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
            return AfterRun::ChildGone;
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
        Err(failure) => {
            tracing::warn!(task_id = %task_id, failure = %failure, "the run frame could not be sent");
            drop(held);
            let lost = lost_after_kill(channel).await;
            answer.send(end_of_loss(shared, channel, lost).await);
            return AfterRun::ChildGone;
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
            AfterRun::ChildGone
        }
        FirstAnswer::Wait(FrameWait::Lost(lost)) => {
            drop(held);
            answer.send(end_of_loss(shared, channel, lost).await);
            AfterRun::ChildGone
        }
        FirstAnswer::Wait(FrameWait::Expired) => {
            drop(held);
            let timed_out = timeout_end(timeout);
            answer.send(end_after_kill(shared, channel, timed_out).await);
            AfterRun::ChildGone
        }
        FirstAnswer::DrainDeadline => {
            drop(held);
            let crashed = AttemptEnd::Crashed {
                reason: FailureDetail::cut("the worker killed the child at the drain deadline"),
            };
            answer.send(end_after_kill(shared, channel, crashed).await);
            AfterRun::ChildGone
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
    match channel.wait_exit(CHILD_KILL_WAIT).await {
        ChildExit::Reaped(_) | ChildExit::ReapedElsewhere => seen_end,
        ChildExit::ExitUnknown => exit_not_seen(shared, channel, seen_end).await,
    }
}

/// A lost child: the crash path once its exit is seen.
async fn end_of_loss(shared: &PoolShared, channel: &ChildChannel, lost: ChildLost) -> AttemptEnd {
    let crashed = AttemptEnd::Crashed {
        reason: FailureDetail::cut(&lost.to_string()),
    };
    match lost.exit {
        ChildExit::Reaped(_) | ChildExit::ReapedElsewhere => crashed,
        ChildExit::ExitUnknown => exit_not_seen(shared, channel, crashed).await,
    }
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
/// every end of the child whose exit is seen.
async fn end_after_return(
    shared: &PoolShared,
    channel: &mut ChildChannel,
    task_id: uuid::Uuid,
    returned: BodyEnd,
    held: crate::worker::runner_beats::HeldDispatch,
    answer: Answer,
) -> AfterRun {
    let returned_end = AttemptEnd::Reported(RunEnd::from(returned));
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
            AfterRun::ChildGone
        }
        FirstAnswer::Wait(FrameWait::Expired) | FirstAnswer::DrainDeadline => {
            drop(held);
            answer.send(end_after_kill(shared, channel, returned_end).await);
            AfterRun::ChildGone
        }
        FirstAnswer::Wait(FrameWait::Frame(other)) => {
            tracing::error!(task_id = %task_id, frame = ?other, "an isolated child sent a frame out of turn");
            drop(held);
            let lost = channel.discard_violating_child().await;
            let end = match lost.exit {
                ChildExit::Reaped(_) | ChildExit::ReapedElsewhere => returned_end,
                ChildExit::ExitUnknown => exit_not_seen(shared, channel, returned_end).await,
            };
            answer.send(end);
            AfterRun::ChildGone
        }
        FirstAnswer::Wait(FrameWait::Lost(lost)) => {
            drop(held);
            let end = match lost.exit {
                ChildExit::Reaped(_) | ChildExit::ReapedElsewhere => returned_end,
                ChildExit::ExitUnknown => exit_not_seen(shared, channel, returned_end).await,
            };
            answer.send(end);
            AfterRun::ChildGone
        }
    }
}

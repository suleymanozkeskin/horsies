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
//!   sent.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::time::Instant;

use super::dispatch::AttemptEnd;
use super::pool::{
    Answer, DownState, ExitNotSeen, IsolatedSlotState, MemoryLimit, PoolFailure, PoolShared,
    QueuedRun, TaskLimit,
};
use super::route::IsolatedDeadline;
use crate::broker::attempt_writes::RetryPlacement;
use crate::worker::executor::protocol::{
    BodyEnd, ChildFrame, FailureDetail, ParentFrame, RssReading, RunEnd, TaskTimeoutMs,
};
use crate::worker::executor::supervisor::{
    boot_end_of, BootCounts, BootDecision, BootEnd, BootFailure, BootFailureCause, BootReply,
    ChildChannel, ChildExit, ChildLost, ExitState, FrameWait, MailboxBound, SpawnError,
    CHILD_KILL_WAIT, CHILD_READY_TIMEOUT,
};

/// After a body returned with work left on its loop, the child reports how
/// that work ended within this time, or it is killed: 1 s of grace, 0.3 s
/// of cancel rounds, the 15 s bound of the finalizing marker, and 10 s of
/// margin (Python `ISOLATED_LEFTOVER_BACKSTOP_S`).
pub const ISOLATED_LEFTOVER_BACKSTOP: Duration = Duration::from_millis(26_300);

/// The end of a boot attempt.
enum BootAttempt {
    Ready(ChildChannel),
    Ended(BootEnd),
    Stopped,
}

/// The end of the boot loop of a slot.
enum Booted {
    Child(ChildChannel),
    PoolStopped,
    PoolFailed,
}

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

/// The slot task: boot a child, serve it, replace it, until the pool stops
/// or fails.
pub(super) async fn run_slot(shared: Arc<PoolShared>, index: usize) {
    loop {
        match boot_child(&shared, index).await {
            Booted::Child(channel) => serve_generation(&shared, index, channel).await,
            Booted::PoolStopped | Booted::PoolFailed => return,
        }
        match shared.stop.is_cancelled() || shared.failed.is_cancelled() {
            true => return,
            false => shared.set(index, IsolatedSlotState::Booting),
        }
    }
}

/// Wait `wait`, or less when the pool stops or fails.
async fn pause(shared: &PoolShared, wait: Duration) {
    tokio::select! {
        () = tokio::time::sleep(wait) => {}
        () = shared.stop.cancelled() => {}
        () = shared.failed.cancelled() => {}
    }
}

/// Boot until a child is ready, by the restart rules.
async fn boot_child(shared: &PoolShared, index: usize) -> Booted {
    let mut counts = BootCounts::default();
    loop {
        match (shared.stop.is_cancelled(), shared.failed.is_cancelled()) {
            (true, _) => return Booted::PoolStopped,
            (false, true) => return Booted::PoolFailed,
            (false, false) => {}
        }
        let failure = match boot_attempt(shared).await {
            BootAttempt::Ready(channel) => {
                if let IsolatedSlotState::Down(down) = shared.state(index) {
                    tracing::warn!(
                        slot = index,
                        failed_boots = down.failed_boots.get(),
                        cause = %down.cause,
                        "an isolated slot is back in service",
                    );
                }
                shared.set(index, IsolatedSlotState::Idle { pid: channel.pid() });
                return Booted::Child(channel);
            }
            BootAttempt::Stopped => return Booted::PoolStopped,
            BootAttempt::Ended(BootEnd::ChildHookFailed) => {
                shared.fail(PoolFailure::ChildHookFailed);
                return Booted::PoolFailed;
            }
            BootAttempt::Ended(BootEnd::Failed(failure)) => failure,
        };
        let failed_boots_while_down = match shared.state(index) {
            IsolatedSlotState::Down(down) => down.failed_boots.get(),
            IsolatedSlotState::Booting
            | IsolatedSlotState::Idle { .. }
            | IsolatedSlotState::Busy { .. }
            | IsolatedSlotState::Retiring => 0,
        };
        let decision = counts.after_failure(
            failure.cause,
            shared.pool_at_failure(index),
            failed_boots_while_down,
        );
        match decision {
            BootDecision::OutsideKillSpared {
                kills_in_a_row,
                wait,
            } => {
                tracing::warn!(
                    slot = index,
                    kills_in_a_row,
                    "a booting child was killed from outside"
                );
                pause(shared, wait).await;
            }
            BootDecision::Retry { counted, wait } => {
                shared.set(index, IsolatedSlotState::Booting);
                tracing::warn!(slot = index, counted, failure = %failure, "a child boot attempt failed");
                pause(shared, wait).await;
            }
            BootDecision::SlotDown {
                counted,
                failed_boots,
                wait,
            } => {
                let down_since = match shared.state(index) {
                    IsolatedSlotState::Down(down) => down.down_since,
                    IsolatedSlotState::Booting
                    | IsolatedSlotState::Idle { .. }
                    | IsolatedSlotState::Busy { .. }
                    | IsolatedSlotState::Retiring => Utc::now(),
                };
                tracing::warn!(
                    slot = index,
                    counted,
                    failed_boots = failed_boots.get(),
                    wait_s = wait.as_secs_f64(),
                    failure = %failure,
                    "an isolated slot is down",
                );
                shared.set(
                    index,
                    IsolatedSlotState::Down(DownState {
                        cause: failure.cause,
                        detail: failure.detail.as_str().to_owned(),
                        down_since,
                        failed_boots,
                    }),
                );
                pause(shared, wait).await;
            }
            BootDecision::Exhausted { attempts } => {
                shared.fail(PoolFailure::BootAttemptsExhausted {
                    attempts,
                    last: failure,
                });
                return Booted::PoolFailed;
            }
        }
    }
}

fn spawn_failure(error: &SpawnError) -> Option<BootFailure> {
    match error {
        SpawnError::ResourceShortage { errno, detail } => Some(BootFailure::new(
            BootFailureCause::ResourceShortage(*errno),
            detail,
        )),
        SpawnError::SpawnFailed { .. } | SpawnError::TooManyChildren | SpawnError::QueueFull => {
            Some(BootFailure::new(
                BootFailureCause::SpawnFailed,
                &error.to_string(),
            ))
        }
        SpawnError::SpawnerStopped => None,
    }
}

/// Start one child, send its init frame, and wait for its ready frame.
async fn boot_attempt(shared: &PoolShared) -> BootAttempt {
    let spawned = match shared.spawner.spawn(shared.config.command.clone()).await {
        Ok(spawned) => spawned,
        Err(error) => {
            return match spawn_failure(&error) {
                Some(failure) => BootAttempt::Ended(BootEnd::Failed(failure)),
                None => BootAttempt::Stopped,
            }
        }
    };
    let child_id = spawned.id;
    let mut channel = match ChildChannel::attach(
        spawned,
        shared.spawner.clone(),
        MailboxBound::for_seats(NonZeroUsize::MIN),
    ) {
        Ok(channel) => channel,
        Err(error) => {
            let _ = shared.spawner.kill_session(child_id).await;
            return BootAttempt::Ended(BootEnd::Failed(BootFailure::new(
                BootFailureCause::SpawnFailed,
                &format!("the child socket could not join the runtime: {error}"),
            )));
        }
    };
    if let Err(failure) = channel
        .send(&ParentFrame::Init(shared.config.init.clone()))
        .await
    {
        tracing::warn!(pid = channel.pid().get(), failure = %failure, "the init frame could not be sent");
        let lost = lost_after_kill(&mut channel).await;
        let _ = channel.dispose().await;
        return BootAttempt::Ended(boot_end_of(BootReply::Lost(lost)));
    }
    let reply = tokio::select! {
        reply = channel.await_frame(Some(CHILD_READY_TIMEOUT)) => reply,
        () = shared.stop.cancelled() => {
            let _ = channel.dispose_lost().await;
            return BootAttempt::Stopped;
        }
    };
    match reply {
        FrameWait::Frame(ChildFrame::Ready(_)) => BootAttempt::Ready(channel),
        FrameWait::Frame(ChildFrame::BootFailed(frame)) => {
            let _ = channel.dispose_lost().await;
            BootAttempt::Ended(boot_end_of(BootReply::BootFailed(frame)))
        }
        FrameWait::Frame(
            ChildFrame::Result(_)
            | ChildFrame::BodyReturned(_)
            | ChildFrame::LeftWorkEnded(_)
            | ChildFrame::LeftWorkStuck(_)
            | ChildFrame::SharedResult(_)
            | ChildFrame::DeadlineFired(_)
            | ChildFrame::SecondCancel(_)
            | ChildFrame::RunStuck(_)
            | ChildFrame::LeftoverCancel(_)
            | ChildFrame::LeftoverStuck(_)
            | ChildFrame::LoopBlocked(_),
        ) => {
            let lost = channel.discard_violating_child().await;
            let _ = channel.dispose().await;
            BootAttempt::Ended(boot_end_of(BootReply::Lost(lost)))
        }
        FrameWait::Lost(lost) => {
            let _ = channel.dispose_lost().await;
            BootAttempt::Ended(boot_end_of(BootReply::Lost(lost)))
        }
        FrameWait::Expired => {
            let _ = channel.dispose_lost().await;
            BootAttempt::Ended(boot_end_of(BootReply::NotReadyInTime))
        }
    }
}

/// Kill the child by request of the worker, wait for its exit, and
/// describe its end.
async fn lost_after_kill(channel: &mut ChildChannel) -> ChildLost {
    let _ = channel.kill_for_caller().await;
    let _ = channel.wait_exit(CHILD_KILL_WAIT).await;
    channel.lost_for(crate::worker::executor::supervisor::LossCause::KilledByParent)
}

/// Serve tasks on one child until it is lost, retires, or the pool stops.
async fn serve_generation(shared: &PoolShared, index: usize, mut channel: ChildChannel) {
    let mut tasks_done: u64 = 0;
    loop {
        shared.set(index, IsolatedSlotState::Idle { pid: channel.pid() });
        let Some(run) = next_run(shared, &channel).await else {
            break;
        };
        shared.set(index, IsolatedSlotState::Busy { pid: channel.pid() });
        tasks_done = tasks_done.saturating_add(1);
        match run_one(shared, &mut channel, run).await {
            AfterRun::Serve { rss } => match recycle_reason(shared, tasks_done, rss) {
                None => {}
                Some(reason) => {
                    tracing::debug!(
                        slot = index,
                        pid = channel.pid().get(),
                        reason,
                        "retiring a child"
                    );
                    break;
                }
            },
            AfterRun::Retire => break,
            AfterRun::ChildGone => {
                shared.set(index, IsolatedSlotState::Retiring);
                let _ = channel.dispose_lost().await;
                return;
            }
        }
    }
    shared.set(index, IsolatedSlotState::Retiring);
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
        let cannot_serve = shared.stop.is_cancelled()
            || shared.failed.is_cancelled()
            || channel.has_exited()
            || channel.channel_closed();
        if cannot_serve {
            return None;
        }
        if let Some(run) = shared.pop() {
            return Some(run);
        }
        tokio::select! {
            () = shared.queued.notified() => {}
            () = shared.stop.cancelled() => {}
            () = shared.failed.cancelled() => {}
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
        () = shared.drain_deadline.cancelled() => FirstAnswer::DrainDeadline,
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
    let held = shared.live.hold(task_id, frame.claimed_at);
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
                    () = shared.stop.cancelled() => return unknown,
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

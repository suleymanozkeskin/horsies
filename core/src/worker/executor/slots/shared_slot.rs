//! One shared slot: boot a child, serve runs on its seats, read the stop
//! reports of each run, kill the child when its rules say so, and give
//! each run its end.
//!
//! Rules of one child:
//! - A frame outside the rules of `seat_runs` is a protocol violation: the
//!   child is killed, and every run of it ends as at a loss.
//! - A `loop_blocked` report kills the child at once.
//! - A closing child is killed once no run in it has time left; each run
//!   then ends past its deadline.
//! - A result frame that says the child ends itself (its core stopped)
//!   closes the child to new runs. Once it holds no run, the slot waits for
//!   its exit, with no stop frame; a run that it still holds ends at its
//!   exit, as at a loss.
//! - A run with no end at its backstop, whose child reported neither the
//!   due step of its stop nor its end, kills the child.
//! - The runner beats of every run of a child end when a kill is sent.
//! - At a loss whose exit was seen, each run gets its verdict. When the
//!   exit is not seen, the end is not known (stale recovery on), or a task
//!   waits for the exit and the runs then take the crash path (off).
//! - A slot defect (a run frame that cannot be encoded, a panic of the slot
//!   task) kills the child; every run of it then takes the crash path, as
//!   at a loss. Repeated defects fail the pools ([`super::defects`]).

use std::sync::Arc;

use tokio::sync::watch;
use tokio::time::Instant;

use super::board::SlotPhase;
use super::booting::{boot_child, BootSlot, Booted};
use super::defects::{
    count_defect, AfterDefect, CatchPanic, GenerationEnd, Served, SlotDefect, SlotDefects,
};
use super::dispatch::{AttemptEnd, AttemptLog, NotRunCause};
use super::pool::{Answer, ExitNotSeen};
use super::seat_runs::{
    backstop_step, end_at_late_backstop_exit, end_at_loss, end_past_deadlines, shared_recycle,
    Backstop, BackstopStep, ChildService, CloseReason, FrameOutOfTurn, LoopBlockStep, LossSight,
    RunStopReport, RunWork, SeatRun, SeatRuns, StopReportFrame, StopStep, SHARED_STUCK_BACKSTOP,
};
use super::shared::{Accepting, QueuedSeatRun, SharedPoolShared, Take};
use crate::worker::executor::protocol::{
    ChildAfterFrame, ChildFrame, ParentFrame, ProcessId, RunRef, SharedResultFrame,
};
use crate::worker::executor::supervisor::{
    log_core_stopped_child, ChildChannel, ChildExit, ChildLost, EndWithoutTask, ExitState,
    FrameWait, LossCause, MailboxBound, SendFailure,
};
use crate::worker::runner_beats::{DispatchRunner, HeldChild, HeldDispatch, RegistryClosed};

/// Whether the runner beats of a run go on.
enum Beats {
    Held(HeldDispatch),
    /// The end was read or a kill was sent.
    Ended,
}

/// What the slot keeps with each run.
pub(super) struct RunHold {
    answer: Answer,
    beats: Beats,
    backstop: Backstop,
    /// The log fields of the attempt.
    log: AttemptLog,
}

/// Why the worker killed the child of a generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillReason {
    BlockedLoop,
    /// The backstop of this run found the child frozen.
    Backstop {
        run: RunRef,
    },
    DrainDeadline,
    /// No run in the closing child had time left.
    PastDeadlines,
    RegistryClosed,
    SendFailed,
    /// The slot hit a defect.
    SlotDefect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillSent {
    NotSent,
    Sent(KillReason),
}

/// Whether a generation told the claim loop that its child is ready
/// (#1148).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadyNotice {
    /// The child has not been open with a free seat yet: no claim pass
    /// counted it.
    Pending,
    /// Noted on the board once. Later free seats come from ends of runs,
    /// whose outcome writes wake the claim loop.
    Noted,
}

/// One child of the slot and its runs.
struct Generation {
    channel: ChildChannel,
    runs: SeatRuns<RunHold>,
    service: ChildService,
    /// What the child said it does after its result frames: once one said
    /// `EndsItself`, the child takes no new run.
    child_after: ChildAfterFrame,
    kill: KillSent,
    tasks_done: u64,
    /// The defect that ended this generation, when one did.
    defect: Option<SlotDefect>,
    ready: ReadyNotice,
}

/// What woke the slot.
enum SlotEvent {
    Frame(FrameWait),
    /// Placement picked this slot.
    Wake,
    /// The pools stop or failed.
    PoolStopping,
    DrainDeadline,
}

/// Whether the generation goes on, and how it ends.
enum Flow {
    Continue,
    /// The closing child has no run: retire it.
    Retire,
    /// The child said that it ends itself and has no run: wait for its
    /// exit.
    AwaitOwnEnd,
    /// The closing child has no run with time left: kill it.
    KillPastDeadlines(CloseReason),
    /// The child is lost: end each run.
    Lost(ChildLost),
}

/// The slot task: boot a child, serve it, replace it, until the pools stop
/// or fail. `local` is the slot's number in its pool. A defect costs the
/// child and its runs; [`SLOT_DEFECT_LIMIT`] defects with no finished task
/// between them fail the pools.
///
/// [`SLOT_DEFECT_LIMIT`]: super::defects::SLOT_DEFECT_LIMIT
pub(super) async fn run_shared_slot(pool: Arc<SharedPoolShared>, local: usize) {
    let index = pool.first + local;
    let mut defects = SlotDefects::default();
    loop {
        let served = CatchPanic::new(boot_and_serve(&pool, local, &mut defects)).await;
        let defect = match served {
            Ok(Served::PoolEnded) => return,
            Ok(Served::Generation(GenerationEnd::Ended)) => None,
            Ok(Served::Generation(GenerationEnd::Defect(defect))) => Some(defect),
            Err(payload) => {
                // The child of the generation was dropped with it; its
                // channel kills it on drop.
                pool.board.set(index, SlotPhase::Retiring);
                Some(SlotDefect::panicked(payload.as_ref()))
            }
        };
        pool.publish_load(local, Accepting::NotOpen, 0);
        if let Some(defect) = defect {
            match count_defect(&pool.board, index, &mut defects, defect) {
                AfterDefect::BootAgain => {}
                AfterDefect::PoolFailed => return,
            }
        }
        match pool.board.not_serving() {
            true => return,
            false => pool.board.set(index, SlotPhase::Booting),
        }
    }
}

/// Boot a child for the slot and serve it.
async fn boot_and_serve(
    pool: &SharedPoolShared,
    local: usize,
    defects: &mut SlotDefects,
) -> Served {
    let boot = BootSlot {
        board: &pool.board,
        index: pool.first + local,
        source: pool.source.as_ref(),
        spawner: &pool.spawner,
        command: &pool.config.command,
        init: &pool.config.init,
        mailbox: MailboxBound::for_seats(pool.seats),
        cpus: &pool.config.child_cpus,
        kill_wait: pool.config.kill_wait,
    };
    match boot_child(&boot).await {
        Booted::Child(channel) => {
            Served::Generation(serve_generation(pool, local, channel, defects).await)
        }
        Booted::PoolStopped | Booted::PoolFailed => Served::PoolEnded,
    }
}

/// Serve runs on one child until it is lost, killed, or closed and drained.
/// A panic while the child serves is a defect: the child is killed, and
/// every run of it ends as at a loss.
async fn serve_generation(
    pool: &SharedPoolShared,
    local: usize,
    channel: ChildChannel,
    defects: &mut SlotDefects,
) -> GenerationEnd {
    let mut generation = Generation {
        channel,
        runs: SeatRuns::new(pool.seats),
        service: ChildService::Open,
        child_after: ChildAfterFrame::ServesOn,
        kill: KillSent::NotSent,
        tasks_done: 0,
        defect: None,
        ready: ReadyNotice::Pending,
    };
    let mut finished_seen: u64 = 0;
    let flow = loop {
        let step = CatchPanic::new(serve_step(pool, local, &mut generation)).await;
        if generation.tasks_done > finished_seen {
            finished_seen = generation.tasks_done;
            defects.task_finished();
        }
        match step {
            Ok(Flow::Continue) => {}
            Ok(ended) => break ended,
            Err(payload) => {
                let defect = SlotDefect::panicked(payload.as_ref());
                break Flow::Lost(kill_at_defect(&mut generation, defect).await);
            }
        }
    };
    pool.board.set(pool.first + local, SlotPhase::Retiring);
    pool.publish_load(local, Accepting::NotOpen, 0);
    let defect = generation.defect.take();
    match flow {
        Flow::Retire => retire(generation).await,
        Flow::AwaitOwnEnd => end_child_that_ends_itself(generation).await,
        Flow::KillPastDeadlines(reason) => kill_past_deadlines(pool, generation, reason).await,
        Flow::Lost(lost) => {
            // Every defect sets `defect` and kills the child: the runs end
            // as at a loss by a defect, not by a kill of the worker.
            let lost = match &defect {
                Some(_) => ChildLost {
                    cause: LossCause::SlotDefect,
                    ..lost
                },
                None => lost,
            };
            lose_child(pool, generation, lost).await
        }
        Flow::Continue => unreachable!("the serve loop ends only with an end of the generation"),
    }
    match defect {
        Some(defect) => GenerationEnd::Defect(defect),
        None => GenerationEnd::Ended,
    }
}

/// One step of a generation: take runs, decide its end, or act on the next
/// event.
async fn serve_step(pool: &SharedPoolShared, local: usize, generation: &mut Generation) -> Flow {
    pull_runs(pool, local, generation).await;
    publish(pool, local, generation);
    note_ready(pool, generation);
    match after_change(pool, generation) {
        Flow::Continue => {}
        ended => return ended,
    }
    match next_event(pool, local, generation).await {
        SlotEvent::Frame(wait) => on_wait(pool, generation, wait).await,
        SlotEvent::Wake => Flow::Continue,
        SlotEvent::PoolStopping => {
            generation.service.close_for_pool_stop();
            Flow::Continue
        }
        SlotEvent::DrainDeadline => {
            let task_ids = generation.runs.task_ids();
            tracing::warn!(
                pid = generation.channel.pid().get(),
                tasks = ?task_ids,
                "killing a shared child that still runs tasks at the drain deadline",
            );
            pool.drain_kills.note(task_ids);
            send_kill(generation, KillReason::DrainDeadline).await;
            Flow::Continue
        }
    }
}

/// After a panic while the child served: note the defect, kill the child,
/// wait for its exit, and describe its end with the defect as its cause
/// (Python `_clear_slot`).
async fn kill_at_defect(generation: &mut Generation, defect: SlotDefect) -> ChildLost {
    generation.defect = Some(defect);
    send_kill(generation, KillReason::SlotDefect).await;
    let kill_wait = generation.channel.kill_wait();
    let _ = generation.channel.wait_exit(kill_wait).await;
    generation.channel.lost_for(LossCause::SlotDefect)
}

/// Publish the slot's phase on the board and its load to placement.
fn publish(pool: &SharedPoolShared, local: usize, generation: &Generation) {
    let pid = generation.channel.pid();
    let runs = generation.runs.count();
    let accepting = match (generation.service, generation.kill, generation.child_after) {
        (ChildService::Open, KillSent::NotSent, ChildAfterFrame::ServesOn) => Accepting::Open,
        (ChildService::Open, KillSent::NotSent, ChildAfterFrame::EndsItself)
        | (ChildService::Open, KillSent::Sent(_), _)
        | (ChildService::Closing(_), _, _) => Accepting::NotOpen,
    };
    let phase = match generation.service {
        ChildService::Open => SlotPhase::Serving { pid },
        ChildService::Closing(_) => SlotPhase::Closing {
            pid,
            runs: u32::try_from(runs).unwrap_or(u32::MAX),
        },
    };
    pool.board.set(pool.first + local, phase);
    pool.publish_load(local, accepting, runs);
}

/// The first time in a generation that the child is open with a free seat,
/// after its load is published: note the ready child on the board once, so
/// the claim loop runs a pass that places a task on it.
fn note_ready(pool: &SharedPoolShared, generation: &mut Generation) {
    let open_with_free_seat = generation.service == ChildService::Open
        && generation.kill == KillSent::NotSent
        && generation.child_after == ChildAfterFrame::ServesOn
        && generation.runs.has_free_seat();
    match (generation.ready, open_with_free_seat) {
        (ReadyNotice::Pending, true) => {
            pool.board.note_child_ready();
            generation.ready = ReadyNotice::Noted;
        }
        (ReadyNotice::Pending, false) | (ReadyNotice::Noted, _) => {}
    }
}

/// Wait for the next frame, a backstop, a placement, the stop or the drain
/// deadline.
async fn next_event(
    pool: &SharedPoolShared,
    local: usize,
    generation: &mut Generation,
) -> SlotEvent {
    let bound = generation
        .runs
        .iter()
        .filter_map(|seat| seat.attached.backstop.due_at())
        .min()
        .map(|due| due.saturating_duration_since(Instant::now()));
    let open = matches!(generation.service, ChildService::Open);
    let drain_kill = generation.kill == KillSent::NotSent && generation.runs.count() > 0;
    tokio::select! {
        wait = generation.channel.await_frame(bound) => SlotEvent::Frame(wait),
        () = pool.wakes[local].notified(), if open => SlotEvent::Wake,
        () = pool.board.stop.cancelled(), if open => SlotEvent::PoolStopping,
        () = pool.board.failed.cancelled(), if open => SlotEvent::PoolStopping,
        () = pool.board.drain_deadline.cancelled(), if drain_kill => SlotEvent::DrainDeadline,
    }
}

/// After every change: wait for the exit of a child that ends itself and
/// has no run, retire a closing child with no run, or kill a closing child
/// with no run that has time left.
fn after_change(pool: &SharedPoolShared, generation: &mut Generation) -> Flow {
    if pool.board.not_serving() {
        generation.service.close_for_pool_stop();
    }
    let ends_with_no_run =
        generation.child_after == ChildAfterFrame::EndsItself && generation.runs.count() == 0;
    if ends_with_no_run {
        return Flow::AwaitOwnEnd;
    }
    let reason = match generation.service {
        ChildService::Open => return Flow::Continue,
        ChildService::Closing(reason) => reason,
    };
    match (
        generation.runs.count(),
        generation.kill,
        generation.runs.all_out_of_time(pool.kind),
    ) {
        (0, _, _) => Flow::Retire,
        (_, KillSent::NotSent, true) => Flow::KillPastDeadlines(reason),
        (_, KillSent::NotSent, false) | (_, KillSent::Sent(_), _) => Flow::Continue,
    }
}

/// End a child that said it ends itself and holds no run: wait for its exit
/// with no stop frame ([`ChildChannel::await_own_end`]), and log an end with
/// the core-stopped code.
async fn end_child_that_ends_itself(generation: Generation) {
    let pid = generation.channel.pid();
    let exit = generation.channel.await_own_end().await;
    match EndWithoutTask::of(exit) {
        EndWithoutTask::CoreStopped => log_core_stopped_child(pid),
        EndWithoutTask::Other => {}
    }
}

/// Retire a child with no run: the stop frame, or the disposal of a child
/// that was killed, exited, or whose channel closed.
async fn retire(generation: Generation) {
    let ended = generation.kill != KillSent::NotSent
        || generation.channel.has_exited()
        || generation.channel.channel_closed();
    match ended {
        true => {
            let _ = generation.channel.dispose_lost().await;
        }
        false => {
            let _ = generation.channel.retire().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Sending runs
// ---------------------------------------------------------------------------

/// Take the tasks that placement gives this slot and send each.
async fn pull_runs(pool: &SharedPoolShared, local: usize, generation: &mut Generation) {
    loop {
        let can_take = generation.service == ChildService::Open
            && generation.kill == KillSent::NotSent
            && generation.child_after == ChildAfterFrame::ServesOn
            && !generation.channel.has_exited()
            && !generation.channel.channel_closed()
            && generation.runs.has_free_seat();
        if !can_take {
            return;
        }
        publish(pool, local, generation);
        match pool.take_for(local) {
            Take::Run(queued) => send_run(pool, generation, queued).await,
            Take::WakeOther(slot) => {
                pool.wakes[slot].notify_one();
                return;
            }
            Take::Nothing => return,
        }
    }
}

/// Hold the run for the runner beats, add it to the seats, and send its
/// frame. A closed registry refuses the run and kills the child; a failed
/// send kills the child.
async fn send_run(pool: &SharedPoolShared, generation: &mut Generation, queued: QueuedSeatRun) {
    let QueuedSeatRun {
        frame,
        mut answer,
        log,
    } = queued;
    let run = RunRef {
        dispatch: frame.dispatch,
        task_id: frame.task_id,
    };
    let channel = &generation.channel;
    let runner = DispatchRunner::Child(HeldChild {
        id: channel.id(),
        pid: channel.pid(),
        spawner: pool.spawner.clone(),
        kill_mark: channel.kill_mark(),
    });
    let held = match pool.live.hold(frame.task_id, frame.claimed_at, runner) {
        Ok(held) => held,
        Err(RegistryClosed) => {
            tracing::error!(
                task_id = %frame.task_id,
                attempt = log.attempt.get(),
                trace_id = log.trace_id.log_field(),
                pid = channel.pid().get(),
                "the runner heartbeats failed closed; the task is not sent, and its child is killed",
            );
            answer.send(AttemptEnd::NotDispatched(
                NotRunCause::RunnerBeatsFailedClosed,
            ));
            send_kill(generation, KillReason::RegistryClosed).await;
            return;
        }
    };
    answer.child_took_it(channel.pid());
    let work = RunWork::of(&frame.work);
    let sent_at = Instant::now();
    generation.runs.add(SeatRun {
        run,
        work,
        sent_at,
        report: RunStopReport::NotReported,
        attached: RunHold {
            answer,
            beats: Beats::Held(held),
            backstop: Backstop::after_send(work, sent_at),
            log,
        },
    });
    let parent_frame = ParentFrame::SharedRun(frame);
    let sent = generation.channel.send(&parent_frame).await;
    // The input is not kept once the frame is written.
    drop(parent_frame);
    match sent {
        Ok(()) => {}
        Err(SendFailure::Encode(error)) => {
            // Nothing reached the child: the frame failed in the slot's own
            // path, which is a defect (Python `_serve_slot`).
            tracing::error!(
                task_id = %run.task_id,
                attempt = log.attempt.get(),
                trace_id = log.trace_id.log_field(),
                error = %error,
                "a shared run frame could not be encoded: {error}",
            );
            generation.defect = Some(SlotDefect::frame_not_encoded(&error));
            send_kill(generation, KillReason::SlotDefect).await;
        }
        Err(failure @ (SendFailure::Write(_) | SendFailure::TimedOut)) => {
            tracing::warn!(
                task_id = %run.task_id,
                attempt = log.attempt.get(),
                trace_id = log.trace_id.log_field(),
                failure = %failure,
                "a shared run frame could not be sent",
            );
            send_kill(generation, KillReason::SendFailed).await;
        }
    }
}

/// Kill the child once: end the runner beats of every run, stop the
/// backstops, and send `SIGKILL` to its session. The loss that follows
/// ends every run.
async fn send_kill(generation: &mut Generation, reason: KillReason) {
    if generation.kill != KillSent::NotSent {
        return;
    }
    generation.kill = KillSent::Sent(reason);
    for hold in generation.runs.attached_mut() {
        hold.beats = Beats::Ended;
        hold.backstop = Backstop::Done;
    }
    let _ = generation.channel.kill_for_caller().await;
}

/// End one run with `end`: its beats end, and its answer goes out.
fn finish(seat: SeatRun<RunHold>, end: AttemptEnd) {
    let RunHold { answer, beats, .. } = seat.attached;
    drop(beats);
    answer.send(end);
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// Act on what the wait for a frame gave.
async fn on_wait(pool: &SharedPoolShared, generation: &mut Generation, wait: FrameWait) -> Flow {
    match wait {
        FrameWait::Frame(frame) => match on_frame(pool, generation, frame).await {
            Ok(()) => Flow::Continue,
            Err(bad) => {
                tracing::error!(
                    pid = generation.channel.pid().get(),
                    error = %bad,
                    "a shared child sent a frame out of turn: {bad}; killing it",
                );
                Flow::Lost(generation.channel.discard_violating_child().await)
            }
        },
        FrameWait::Lost(lost) => Flow::Lost(lost),
        FrameWait::Expired => {
            run_backstops(generation).await;
            Flow::Continue
        }
    }
}

/// A frame that names no run of the frame rules, or a frame of another
/// child kind.
#[derive(Debug, thiserror::Error)]
enum BadFrame {
    #[error(transparent)]
    OutOfTurn(FrameOutOfTurn),
    #[error("a shared child sent a {0} frame")]
    WrongKind(&'static str),
}

/// Apply one frame of the child to its runs.
async fn on_frame(
    pool: &SharedPoolShared,
    generation: &mut Generation,
    frame: ChildFrame,
) -> Result<(), BadFrame> {
    let pid = generation.channel.pid().get();
    match frame {
        ChildFrame::SharedResult(result) => on_result(pool, generation, result),
        ChildFrame::DeadlineFired(run) => {
            let timeout = generation
                .runs
                .note_deadline(run)
                .map_err(BadFrame::OutOfTurn)?;
            let log = generation
                .runs
                .get(run.dispatch)
                .map(|seat| seat.attached.log);
            tracing::info!(
                task_id = %run.task_id,
                attempt = log.map(|log| log.attempt.get()),
                trace_id = log.and_then(|log| log.trace_id.log_field()),
                timeout_ms = timeout.get(),
                pid,
                "a task reached its timeout_ms",
            );
            let before = generation.service;
            generation.service.close_at_deadline(pool.kind, run);
            if before == ChildService::Open && generation.service != before {
                tracing::warn!(
                    task_id = %run.task_id,
                    attempt = log.map(|log| log.attempt.get()),
                    trace_id = log.and_then(|log| log.trace_id.log_field()),
                    pid,
                    "closing a shared thread child: a task passed its deadline, and a thread \
                     cannot be cancelled; the child is killed when no task in it has time left",
                );
            }
            Ok(())
        }
        ChildFrame::SecondCancel(run) => {
            on_stop_report(generation, StopReportFrame::SecondCancel(run))
        }
        ChildFrame::RunStuck(stuck) => on_stop_report(generation, StopReportFrame::RunStuck(stuck)),
        ChildFrame::LeftoverCancel(cancel) => {
            on_stop_report(generation, StopReportFrame::LeftoverCancel(cancel))
        }
        ChildFrame::LeftoverStuck(stuck) => {
            on_stop_report(generation, StopReportFrame::LeftoverStuck(stuck))
        }
        ChildFrame::LoopBlocked(blocked) => {
            match generation
                .runs
                .note_loop_blocked(&blocked)
                .map_err(BadFrame::OutOfTurn)?
            {
                LoopBlockStep::AlreadyKilled => Ok(()),
                LoopBlockStep::KillNow { holder } => {
                    generation.service.close_for_stop(CloseReason::LoopBlocked);
                    tracing::error!(
                        pid,
                        blocked_ms = blocked.blocked_ms,
                        holder = ?holder,
                        location = %blocked.location,
                        tasks = generation.runs.count(),
                        "the event loop of a shared child did not turn; killing the child",
                    );
                    send_kill(generation, KillReason::BlockedLoop).await;
                    Ok(())
                }
            }
        }
        ChildFrame::Ready(_) => Err(BadFrame::WrongKind("ready")),
        ChildFrame::BootFailed(_) => Err(BadFrame::WrongKind("boot_failed")),
        ChildFrame::Result(_) => Err(BadFrame::WrongKind("result")),
        ChildFrame::BodyReturned(_) => Err(BadFrame::WrongKind("body_returned")),
        ChildFrame::LeftWorkEnded(_) => Err(BadFrame::WrongKind("left_work_ended")),
        ChildFrame::LeftWorkStuck(_) => Err(BadFrame::WrongKind("left_work_stuck")),
    }
}

/// A run ended: answer it, count it, note a child that ends itself, and
/// close the child at a recycle limit.
fn on_result(
    pool: &SharedPoolShared,
    generation: &mut Generation,
    result: SharedResultFrame,
) -> Result<(), BadFrame> {
    let rss = result.rss;
    let child = result.child;
    let (seat, end) = generation
        .runs
        .end_run(result)
        .map_err(BadFrame::OutOfTurn)?;
    finish(seat, end);
    generation.child_after = match (generation.child_after, child) {
        (ChildAfterFrame::EndsItself, _) | (_, ChildAfterFrame::EndsItself) => {
            ChildAfterFrame::EndsItself
        }
        (ChildAfterFrame::ServesOn, ChildAfterFrame::ServesOn) => ChildAfterFrame::ServesOn,
    };
    generation.tasks_done = generation.tasks_done.saturating_add(1);
    let recycle = shared_recycle(
        pool.config.tasks_per_seat,
        pool.config.memory_per_child,
        pool.seats,
        generation.tasks_done,
        rss,
    );
    generation.service.close_after_task(recycle);
    Ok(())
}

/// Record a step of the stop of an async run, and close the child.
fn on_stop_report(generation: &mut Generation, report: StopReportFrame) -> Result<(), BadFrame> {
    let pid = generation.channel.pid().get();
    match generation
        .runs
        .note_stop_report(&report)
        .map_err(BadFrame::OutOfTurn)?
    {
        StopStep::Close(reason) => {
            generation.service.close_for_stop(reason);
            tracing::warn!(pid, report = ?report, "closing a shared child: {reason}");
        }
        StopStep::Recorded => {
            tracing::debug!(
                pid,
                report = ?report,
                "a task returned, and the work it left is stuck; it ends with what it returned",
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Backstops
// ---------------------------------------------------------------------------

/// Act on every backstop that came due: kill a frozen child, or wait more.
async fn run_backstops(generation: &mut Generation) {
    let now = Instant::now();
    let mut frozen = Vec::new();
    for seat in generation.runs.iter() {
        let backstop = seat.attached.backstop;
        if backstop.due_at().is_some_and(|due| due <= now) {
            frozen.push((
                seat.run,
                seat.attached.log,
                backstop_step(backstop, seat.work, &seat.report),
            ));
        }
    }
    for (run, log, step) in frozen {
        let next = match step {
            BackstopStep::KillChild => {
                tracing::warn!(
                    task_id = %run.task_id,
                    attempt = log.attempt.get(),
                    trace_id = log.trace_id.log_field(),
                    pid = generation.channel.pid().get(),
                    "a task passed its backstop, and its shared child reported neither the due \
                     step of its stop nor its end; killing the child with every task of it",
                );
                send_kill(generation, KillReason::Backstop { run }).await;
                return;
            }
            BackstopStep::CheckStuckLater => Backstop::Stuck {
                at: now + SHARED_STUCK_BACKSTOP,
            },
            BackstopStep::WaitForEnd => Backstop::Done,
        };
        if let Some(hold) = generation.runs.attached_of(run.dispatch) {
            hold.backstop = next;
        }
    }
}

// ---------------------------------------------------------------------------
// Ends of a child
// ---------------------------------------------------------------------------

/// Kill a closing child once no run in it has time left, read what it
/// wrote until its exit, and end each run past its deadline (Python
/// `_kill_past_deadlines`).
async fn kill_past_deadlines(
    pool: &SharedPoolShared,
    mut generation: Generation,
    reason: CloseReason,
) {
    tracing::warn!(
        pid = generation.channel.pid().get(),
        tasks = generation.runs.count(),
        "killing a shared child: no task in it has time left; {reason}",
    );
    send_kill(&mut generation, KillReason::PastDeadlines).await;
    let lost = read_until_exit(&mut generation).await;
    let exit = generation.channel.exit_watch();
    let seats = generation.runs.drain();
    match LossSight::of(lost) {
        LossSight::Seen(_) => {
            for seat in seats {
                let end = end_past_deadlines(&seat, pool.kind, reason);
                finish(seat, end);
            }
        }
        LossSight::NotSeen(lost) => {
            answer_without_seen_exit(pool, seats, lost, generation.kill, exit)
        }
    }
    let _ = generation.channel.dispose_lost().await;
}

/// Read the frames of a killed child until its loss, at most
/// the kill wait of the child. A result ends its run; other frames are dropped,
/// because the child is ending.
async fn read_until_exit(generation: &mut Generation) -> ChildLost {
    let until = Instant::now() + generation.channel.kill_wait();
    loop {
        let remaining = until.saturating_duration_since(Instant::now());
        match generation.channel.await_frame(Some(remaining)).await {
            FrameWait::Frame(ChildFrame::SharedResult(result)) => {
                if let Ok((seat, end)) = generation.runs.end_run(result) {
                    finish(seat, end);
                }
            }
            FrameWait::Frame(_) => {}
            FrameWait::Lost(lost) => return lost,
            FrameWait::Expired => return generation.channel.lost_for(LossCause::KilledByParent),
        }
    }
}

/// End every run of a lost child (Python `_lose_shared_child`,
/// `_end_all_runs`).
///
/// A child that held no run and ended with
/// [`ChildExitCode::CoreStopped`](crate::worker::executor::protocol::ChildExitCode)
/// is logged at error level once its exit is seen; every other loss logs a
/// warning.
async fn lose_child(pool: &SharedPoolShared, mut generation: Generation, lost: ChildLost) {
    let pid = generation.channel.pid();
    let held = generation.runs.count();
    match held {
        0 => {}
        _ => warn_lost_shared_child(pid, held, &lost),
    }
    let closer = generation.service.close_culprit();
    let kill = generation.runs.kill().clone();
    let exit = generation.channel.exit_watch();
    let seats = generation.runs.drain();
    match LossSight::of(lost) {
        LossSight::Seen(loss) => {
            let now = Instant::now();
            for seat in seats {
                let end = end_at_loss(&seat, &kill, closer, loss, now);
                finish(seat, end);
            }
        }
        LossSight::NotSeen(lost) => {
            answer_without_seen_exit(pool, seats, lost, generation.kill, exit)
        }
    }
    let exit = generation.channel.dispose_lost().await;
    match (held, EndWithoutTask::of(exit)) {
        (0, EndWithoutTask::CoreStopped) => log_core_stopped_child(pid),
        (0, EndWithoutTask::Other) => warn_lost_shared_child(pid, held, &lost),
        (_, EndWithoutTask::CoreStopped | EndWithoutTask::Other) => {}
    }
}

/// The warning of a lost shared child.
fn warn_lost_shared_child(pid: ProcessId, held: usize, lost: &ChildLost) {
    tracing::warn!(
        pid = pid.get(),
        tasks = held,
        lost = %lost,
        "lost a shared child",
    );
}

/// The runs of a child whose exit was not seen after a kill: the end is
/// not known (stale recovery on), or a task waits for the exit as long as
/// the pools live (off). After a late exit, the run whose backstop killed
/// the child gets the late backstop end, and every other run the crash
/// path (Python `_wait_for_unseen_exit`, `_outcome_at_late_backstop_exit`).
fn answer_without_seen_exit(
    pool: &SharedPoolShared,
    seats: Vec<SeatRun<RunHold>>,
    lost: ChildLost,
    kill: KillSent,
    exit: watch::Receiver<ExitState>,
) {
    let unknown = AttemptEnd::EndUnknown { pid: lost.pid };
    match pool.config.exit_not_seen {
        ExitNotSeen::LeaveToRecovery => {
            for seat in seats {
                finish(seat, unknown.clone());
            }
        }
        ExitNotSeen::WaitForExit => {
            // A kill was sent to the child, so no beat covers its runs.
            let seats: Vec<SeatRun<RunHold>> = seats
                .into_iter()
                .map(|mut seat| {
                    seat.attached.beats = Beats::Ended;
                    seat
                })
                .collect();
            let stop = pool.board.stop.clone();
            tokio::spawn(answer_after_exit(seats, lost, kill, exit, stop));
        }
    }
}

async fn answer_after_exit(
    seats: Vec<SeatRun<RunHold>>,
    lost: ChildLost,
    kill: KillSent,
    mut exit: watch::Receiver<ExitState>,
    stop: tokio_util::sync::CancellationToken,
) {
    let seen = tokio::select! {
        state = exit.wait_for(|state| *state != ExitState::Running) => match state.map(|state| *state) {
            Ok(ExitState::Reaped(reaped)) => ChildExit::Reaped(reaped),
            Ok(ExitState::ReapedElsewhere) => ChildExit::ReapedElsewhere,
            Ok(ExitState::Running) | Err(_) => ChildExit::ExitUnknown,
        },
        () = stop.cancelled() => ChildExit::ExitUnknown,
    };
    let late = ChildLost { exit: seen, ..lost };
    let loss = match LossSight::of(late) {
        LossSight::Seen(loss) => loss,
        LossSight::NotSeen(_) => {
            for seat in seats {
                finish(seat, AttemptEnd::EndUnknown { pid: lost.pid });
            }
            return;
        }
    };
    for seat in seats {
        let end = match kill {
            KillSent::Sent(KillReason::Backstop { run }) if run == seat.run => {
                end_at_late_backstop_exit(seat.work, seat.run, loss)
            }
            KillSent::Sent(_) | KillSent::NotSent => AttemptEnd::of_lost_child(&late),
        };
        finish(seat, end);
    }
}

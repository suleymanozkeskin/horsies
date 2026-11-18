//! The boot of one slot's child, of any kind, by the restart rules
//! (Python `core/worker/slots.py`, `_boot_child`, `_boot_attempt`). The
//! rules read every slot of the worker on the [`PoolBoard`].

use std::num::NonZeroU32;
use std::time::Duration;

use chrono::Utc;

use super::board::{DownState, PoolBoard, PoolFailure, ReadyChild, SlotPhase};
use crate::worker::executor::placement::{place_process, ChildCpus};
use crate::worker::executor::protocol::{ChildFrame, InitFrame, ParentFrame, RssReading};
use crate::worker::executor::supervisor::{
    boot_end_of, wait_for_exit, BootCounts, BootDecision, BootEnd, BootFailure, BootFailureCause,
    BootReply, ChildChannel, ChildCommand, ChildLost, ChildSource, FrameWait, LastRecord,
    LossCause, MailboxBound, SpawnError, SpawnedChild, SpawnerHandle, CHILD_BOOT_ATTEMPTS,
    CHILD_READY_TIMEOUT,
};

/// What a booting slot needs.
pub(super) struct BootSlot<'a> {
    pub board: &'a PoolBoard,
    /// The slot's index on the board.
    pub index: usize,
    /// Where the slot's children come from.
    pub source: &'a dyn ChildSource,
    /// The spawner of every child: its kills.
    pub spawner: &'a SpawnerHandle,
    pub command: &'a ChildCommand,
    /// The first frame of the slot's children; its kind is the slot's.
    pub init: &'a InitFrame,
    pub mailbox: MailboxBound,
    /// The CPUs of the slot's children, set before the init frame.
    pub cpus: &'a ChildCpus,
    /// The longest wait for the exit of a child after a `SIGKILL`.
    pub kill_wait: Duration,
}

/// The end of a boot attempt.
enum BootAttempt {
    /// The child sent its ready frame, with the RSS it read then.
    Ready(ChildChannel, RssReading),
    Ended(BootEnd),
    Stopped,
}

/// The end of the boot loop of a slot.
pub(super) enum Booted {
    Child(ChildChannel),
    PoolStopped,
    PoolFailed,
}

/// Wait `wait`, or less when the pools stop or fail.
pub(super) async fn pause(board: &PoolBoard, wait: Duration) {
    tokio::select! {
        () = tokio::time::sleep(wait) => {}
        () = board.stop.cancelled() => {}
        () = board.failed.cancelled() => {}
    }
}

/// Boot until a child is ready, by the restart rules.
pub(super) async fn boot_child(slot: &BootSlot<'_>) -> Booted {
    let board = slot.board;
    let index = slot.index;
    let mut counts = BootCounts::default();
    loop {
        match (board.stop.is_cancelled(), board.failed.is_cancelled()) {
            (true, _) => return Booted::PoolStopped,
            (false, true) => return Booted::PoolFailed,
            (false, false) => {}
        }
        let failure = match boot_attempt(slot).await {
            BootAttempt::Ready(channel, rss) => {
                if let SlotPhase::Down(down) = board.phase(index) {
                    tracing::warn!(
                        slot = index,
                        failed_boots = down.failed_boots.get(),
                        cause = %down.cause,
                        "{}",
                        back_in_service_line(index, &down),
                    );
                }
                board.ready(
                    index,
                    ReadyChild {
                        pid: channel.pid(),
                        rss,
                    },
                );
                return Booted::Child(channel);
            }
            BootAttempt::Stopped => return Booted::PoolStopped,
            BootAttempt::Ended(BootEnd::ChildHookFailed) => {
                board.fail(PoolFailure::ChildHookFailed);
                return Booted::PoolFailed;
            }
            BootAttempt::Ended(BootEnd::Failed(failure)) => failure,
        };
        match after_failed_boot(board, index, &mut counts, failure).await {
            AfterFailedBoot::BootAgain => {}
            AfterFailedBoot::PoolFailed => return Booted::PoolFailed,
        }
    }
}

/// What a slot does after a failed boot.
enum AfterFailedBoot {
    BootAgain,
    PoolFailed,
}

fn failed_boots_while_down(phase: &SlotPhase) -> u32 {
    match phase {
        SlotPhase::Down(down) => down.failed_boots.get(),
        SlotPhase::Booting
        | SlotPhase::Serving { .. }
        | SlotPhase::Closing { .. }
        | SlotPhase::Retiring => 0,
    }
}

/// Decide by the restart rules, record the failed boot, and wait (Python
/// `_boot_child`). Every counted attempt is recorded, the last one too,
/// before the pool fails.
async fn after_failed_boot(
    board: &PoolBoard,
    index: usize,
    counts: &mut BootCounts,
    failure: BootFailure,
) -> AfterFailedBoot {
    let phase = board.phase(index);
    let decision = counts.after_failure(
        failure.cause,
        board.pool_at_failure(index),
        failed_boots_while_down(&phase),
    );
    match decision {
        BootDecision::OutsideKillSpared {
            kills_in_a_row,
            wait,
        } => {
            tracing::warn!(
                slot = index,
                kills_in_a_row,
                "{}",
                outside_kill_line(index, kills_in_a_row),
            );
            pause(board, wait).await;
        }
        BootDecision::Retry { counted, wait } => {
            record_attempt_failed(board, index, counted, &failure);
            pause(board, wait).await;
        }
        BootDecision::SlotDown {
            counted,
            failed_boots,
            wait,
        } => {
            let down = DownRecord {
                counted,
                failed_boots,
                wait,
            };
            record_slot_down(board, index, &phase, down, &failure);
            pause(board, wait).await;
        }
        BootDecision::Exhausted { attempts, last } => {
            match last {
                LastRecord::AttemptFailed => {
                    record_attempt_failed(board, index, attempts, &failure);
                }
                LastRecord::SlotDown { failed_boots, wait } => {
                    let down = DownRecord {
                        counted: attempts,
                        failed_boots,
                        wait,
                    };
                    record_slot_down(board, index, &phase, down, &failure);
                }
            }
            board.fail(PoolFailure::BootAttemptsExhausted {
                attempts,
                last: failure,
            });
            return AfterFailedBoot::PoolFailed;
        }
    }
    AfterFailedBoot::BootAgain
}

/// The counts of a failed boot that put a slot down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DownRecord {
    /// The counted attempts of the slot, with this one.
    pub counted: u32,
    /// The failed boots of the slot's down state, with this one.
    pub failed_boots: NonZeroU32,
    /// The wait before the next boot.
    pub wait: Duration,
}

/// Record a failed attempt of a cause that repeats: the slot boots again
/// (Python `_record_failed_boot`).
fn record_attempt_failed(board: &PoolBoard, index: usize, counted: u32, failure: &BootFailure) {
    board.set(index, SlotPhase::Booting);
    tracing::warn!(
        slot = index,
        counted,
        failure = %failure,
        "{}",
        attempt_failed_line(index, counted, failure),
    );
}

/// Put the slot down after a failed boot of a cause that can clear, and log
/// it (Python `_mark_down`). A slot that is already down (`phase`) keeps its
/// `down_since`.
fn record_slot_down(
    board: &PoolBoard,
    index: usize,
    phase: &SlotPhase,
    down: DownRecord,
    failure: &BootFailure,
) {
    let down_since = match phase {
        SlotPhase::Down(state) => state.down_since,
        SlotPhase::Booting
        | SlotPhase::Serving { .. }
        | SlotPhase::Closing { .. }
        | SlotPhase::Retiring => Utc::now(),
    };
    tracing::warn!(
        slot = index,
        counted = down.counted,
        failed_boots = down.failed_boots.get(),
        wait_s = down.wait.as_secs_f64(),
        failure = %failure,
        "{}",
        slot_down_line(index, down, failure),
    );
    board.set(
        index,
        SlotPhase::Down(DownState {
            cause: failure.cause,
            detail: failure.detail.as_str().to_owned(),
            down_since,
            failed_boots: down.failed_boots,
        }),
    );
}

/// The log line of a slot that is down (Python `_mark_down`): the slot,
/// the cause, the detail, the failed boots, the counted attempts and the
/// wait before the next boot. Pure.
pub(super) fn slot_down_line(index: usize, down: DownRecord, failure: &BootFailure) -> String {
    format!(
        "Slot {index} is down: {failure}. Failed boots: {}, counted attempts: \
         {}/{CHILD_BOOT_ATTEMPTS}. Next boot in {:.1}s",
        down.failed_boots.get(),
        down.counted,
        down.wait.as_secs_f64(),
    )
}

/// The log line of a failed attempt of a cause that repeats (Python
/// `_record_failed_boot`). Pure.
pub(super) fn attempt_failed_line(index: usize, counted: u32, failure: &BootFailure) -> String {
    format!("Child boot attempt {counted}/{CHILD_BOOT_ATTEMPTS} failed on slot {index}: {failure}")
}

/// The log line of a down slot whose child is ready (Python
/// `_log_back_in_service`). Pure.
pub(super) fn back_in_service_line(index: usize, down: &DownState) -> String {
    format!(
        "Slot {index} is back in service after {} failed boots ({})",
        down.failed_boots.get(),
        down.cause,
    )
}

/// The log line of a booting replacement child that a kill from outside
/// ended while another child serves (Python `_boot_child`). Pure.
pub(super) fn outside_kill_line(index: usize, kills_in_a_row: u32) -> String {
    format!("Replacement child on slot {index} was killed during boot, {kills_in_a_row} in a row")
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
async fn boot_attempt(slot: &BootSlot<'_>) -> BootAttempt {
    let spawned = match slot.source.spawn(slot.command.clone()).await {
        Ok(spawned) => spawned,
        Err(error) => {
            return match spawn_failure(&error) {
                Some(failure) => BootAttempt::Ended(BootEnd::Failed(failure)),
                None => BootAttempt::Stopped,
            }
        }
    };
    let spawned = match place_child(slot, spawned).await {
        Ok(spawned) => spawned,
        Err(failure) => return BootAttempt::Ended(BootEnd::Failed(failure)),
    };
    let child_id = spawned.id;
    let attached =
        ChildChannel::attach(spawned, slot.spawner.clone(), slot.mailbox, slot.kill_wait);
    let mut channel = match attached {
        Ok(channel) => channel,
        Err(error) => {
            let _ = slot.spawner.kill_session(child_id).await;
            return BootAttempt::Ended(BootEnd::Failed(BootFailure::new(
                BootFailureCause::SpawnFailed,
                &format!("the child socket could not join the runtime: {error}"),
            )));
        }
    };
    if let Err(failure) = channel.send(&ParentFrame::Init(slot.init.clone())).await {
        tracing::warn!(pid = channel.pid().get(), failure = %failure, "the init frame could not be sent");
        let lost = lost_after_kill(&mut channel).await;
        let _ = channel.dispose().await;
        return BootAttempt::Ended(boot_end_of(BootReply::Lost(lost)));
    }
    let reply = tokio::select! {
        reply = channel.await_frame(Some(CHILD_READY_TIMEOUT)) => reply,
        () = slot.board.stop.cancelled() => {
            let _ = channel.dispose_lost().await;
            return BootAttempt::Stopped;
        }
    };
    boot_reply(channel, reply).await
}

/// Set the CPUs of a started child before its init frame (Python
/// `Child._attach_registered`). The child waits for the init frame before it
/// starts a thread, so the CPUs hold for each thread it starts. A child
/// whose CPUs the OS refuses is killed and waited for: its start failed
/// (`SpawnFailed`), and the same placement fails again.
async fn place_child(
    slot: &BootSlot<'_>,
    spawned: SpawnedChild,
) -> Result<SpawnedChild, BootFailure> {
    let cpus = match slot.cpus {
        ChildCpus::Inherited => return Ok(spawned),
        ChildCpus::Set(cpus) => cpus,
    };
    let refused = match place_process(spawned.pid, cpus) {
        Ok(()) => return Ok(spawned),
        Err(refused) => refused,
    };
    let SpawnedChild {
        id,
        socket,
        mut exit,
        ..
    } = spawned;
    drop(socket);
    let _ = slot.spawner.kill_session(id).await;
    let _ = wait_for_exit(&mut exit, slot.kill_wait).await;
    Err(BootFailure::new(
        BootFailureCause::SpawnFailed,
        &refused.to_string(),
    ))
}

/// Read the first answer of a booting child.
async fn boot_reply(mut channel: ChildChannel, reply: FrameWait) -> BootAttempt {
    match reply {
        FrameWait::Frame(ChildFrame::Ready(ready)) => BootAttempt::Ready(channel, ready.rss),
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
pub(super) async fn lost_after_kill(channel: &mut ChildChannel) -> ChildLost {
    let _ = channel.kill_for_caller().await;
    let _ = channel.wait_exit(channel.kill_wait()).await;
    channel.lost_for(LossCause::KilledByParent)
}

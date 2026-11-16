//! The boot of one slot's child, of any kind, by the restart rules
//! (Python `core/worker/slots.py`, `_boot_child`, `_boot_attempt`). The
//! rules read every slot of the worker on the [`PoolBoard`].

use std::time::Duration;

use chrono::Utc;

use super::board::{DownState, PoolBoard, PoolFailure, SlotPhase};
use crate::worker::executor::protocol::{ChildFrame, InitFrame, ParentFrame};
use crate::worker::executor::supervisor::{
    boot_end_of, BootCounts, BootDecision, BootEnd, BootFailure, BootFailureCause, BootReply,
    ChildChannel, ChildCommand, ChildLost, FrameWait, LossCause, MailboxBound, SpawnError,
    SpawnerHandle, CHILD_KILL_WAIT, CHILD_READY_TIMEOUT,
};

/// What a booting slot needs.
pub(super) struct BootSlot<'a> {
    pub board: &'a PoolBoard,
    /// The slot's index on the board.
    pub index: usize,
    pub spawner: &'a SpawnerHandle,
    pub command: &'a ChildCommand,
    /// The first frame of the slot's children; its kind is the slot's.
    pub init: &'a InitFrame,
    pub mailbox: MailboxBound,
}

/// The end of a boot attempt.
enum BootAttempt {
    Ready(ChildChannel),
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
            BootAttempt::Ready(channel) => {
                if let SlotPhase::Down(down) = board.phase(index) {
                    tracing::warn!(
                        slot = index,
                        failed_boots = down.failed_boots.get(),
                        cause = %down.cause,
                        "a slot is back in service",
                    );
                }
                board.set(index, SlotPhase::Serving { pid: channel.pid() });
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

/// Decide by the restart rules, record the slot's phase, and wait.
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
                "a booting child was killed from outside"
            );
            pause(board, wait).await;
        }
        BootDecision::Retry { counted, wait } => {
            board.set(index, SlotPhase::Booting);
            tracing::warn!(slot = index, counted, failure = %failure, "a child boot attempt failed");
            pause(board, wait).await;
        }
        BootDecision::SlotDown {
            counted,
            failed_boots,
            wait,
        } => {
            let down_since = match phase {
                SlotPhase::Down(down) => down.down_since,
                SlotPhase::Booting
                | SlotPhase::Serving { .. }
                | SlotPhase::Closing { .. }
                | SlotPhase::Retiring => Utc::now(),
            };
            tracing::warn!(
                slot = index,
                counted,
                failed_boots = failed_boots.get(),
                wait_s = wait.as_secs_f64(),
                failure = %failure,
                "a slot is down",
            );
            board.set(
                index,
                SlotPhase::Down(DownState {
                    cause: failure.cause,
                    detail: failure.detail.as_str().to_owned(),
                    down_since,
                    failed_boots,
                }),
            );
            pause(board, wait).await;
        }
        BootDecision::Exhausted { attempts } => {
            board.fail(PoolFailure::BootAttemptsExhausted {
                attempts,
                last: failure,
            });
            return AfterFailedBoot::PoolFailed;
        }
    }
    AfterFailedBoot::BootAgain
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
    let spawned = match slot.spawner.spawn(slot.command.clone()).await {
        Ok(spawned) => spawned,
        Err(error) => {
            return match spawn_failure(&error) {
                Some(failure) => BootAttempt::Ended(BootEnd::Failed(failure)),
                None => BootAttempt::Stopped,
            }
        }
    };
    let child_id = spawned.id;
    let mut channel = match ChildChannel::attach(spawned, slot.spawner.clone(), slot.mailbox) {
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

/// Read the first answer of a booting child.
async fn boot_reply(mut channel: ChildChannel, reply: FrameWait) -> BootAttempt {
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
pub(super) async fn lost_after_kill(channel: &mut ChildChannel) -> ChildLost {
    let _ = channel.kill_for_caller().await;
    let _ = channel.wait_exit(CHILD_KILL_WAIT).await;
    channel.lost_for(LossCause::KilledByParent)
}

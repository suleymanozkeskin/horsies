//! The replacement of the child of a slot: why the child of a generation
//! left, and the point that tells the slot board's telemetry when the slot
//! boots a new child in its place (`ChildReplaced`).
//!
//! A slot task tells a replacement once per generation, after the
//! generation ended and only when the pools still serve: a child that left
//! at the stop or the failure of the pools is not replaced, and a boot that
//! gave no child replaced none.

use super::board::PoolBoard;
use crate::telemetry::labels::ReplaceReason;
use crate::worker::executor::protocol::ProcessId;
use crate::worker::executor::supervisor::{ChildLost, EndWithoutTask, LossCause};

/// The child of the current generation of a slot, as the slot task knows
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GenerationChild {
    /// The boot of the generation gave no child yet.
    NotBooted,
    /// The boot gave the child with this process id.
    Booted(ProcessId),
}

/// Why the child of a generation left its slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildLeave {
    /// The child retired, ended itself or was lost, for this reason. The
    /// slot boots a new child unless the pools stop.
    For(ReplaceReason),
    /// The pools stop or failed: the slot boots no new child.
    AtPoolStop,
}

/// The replace reason of a child lost by `lost`: its loss cause, and
/// [`ReplaceReason::CoreStopped`] for a child that died with the
/// core-stopped exit code. Pure.
pub(super) fn reason_of_loss(lost: &ChildLost) -> ReplaceReason {
    match (lost.cause, EndWithoutTask::of(lost.exit)) {
        (LossCause::ChildDied, EndWithoutTask::CoreStopped) => ReplaceReason::CoreStopped,
        (LossCause::ChildDied, EndWithoutTask::Other) => ReplaceReason::ChildDied,
        (LossCause::ChannelLost, _) => ReplaceReason::ChannelLost,
        (LossCause::ProtocolViolation, _) => ReplaceReason::ProtocolViolation,
        (LossCause::ProtocolMismatch(_), _) => ReplaceReason::ProtocolMismatch,
        (LossCause::KilledByParent, _) => ReplaceReason::KilledByParent,
        (LossCause::SlotDefect, _) => ReplaceReason::SlotDefect,
        (LossCause::SpawnerStopped, _) => ReplaceReason::SpawnerStopped,
    }
}

/// Tell the telemetry of `board` that slot `index` replaces `child`, which
/// left for `leave`. The caller boots the next child of the slot after it.
/// A generation with no booted child, and a child that left at the stop of
/// the pools, tell nothing.
pub(super) fn tell_replaced(
    board: &PoolBoard,
    index: usize,
    child: GenerationChild,
    leave: ChildLeave,
) {
    match (child, leave) {
        (GenerationChild::Booted(pid), ChildLeave::For(reason)) => {
            board
                .telemetry()
                .child_replaced(board.class_of(index), index, pid, reason);
        }
        (GenerationChild::NotBooted, _) | (GenerationChild::Booted(_), ChildLeave::AtPoolStop) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::executor::protocol::{ReceivedProtocol, CORE_STOPPED_EXIT_CODE};
    use crate::worker::executor::supervisor::{ChildExit, PartialFrame, ReapedExit};

    fn lost(cause: LossCause, exit: ChildExit) -> ChildLost {
        ChildLost {
            cause,
            exit,
            partial: PartialFrame::NoneAtHand,
            pid: ProcessId::new(42).unwrap(),
        }
    }

    fn exited_with(code: i32) -> ChildExit {
        ChildExit::Reaped(ReapedExit::ExitedWith(code))
    }

    #[test]
    fn each_loss_cause_is_its_own_replace_reason() {
        let killed = ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGKILL));
        let cases = [
            (
                LossCause::ChildDied,
                exited_with(3),
                ReplaceReason::ChildDied,
            ),
            (LossCause::ChannelLost, killed, ReplaceReason::ChannelLost),
            (
                LossCause::ProtocolViolation,
                killed,
                ReplaceReason::ProtocolViolation,
            ),
            (
                LossCause::ProtocolMismatch(ReceivedProtocol::NotNamed),
                exited_with(3),
                ReplaceReason::ProtocolMismatch,
            ),
            (
                LossCause::KilledByParent,
                killed,
                ReplaceReason::KilledByParent,
            ),
            (LossCause::SlotDefect, killed, ReplaceReason::SlotDefect),
            (
                LossCause::SpawnerStopped,
                ChildExit::ExitUnknown,
                ReplaceReason::SpawnerStopped,
            ),
        ];
        for (cause, exit, reason) in cases {
            assert_eq!(reason_of_loss(&lost(cause, exit)), reason, "{cause:?}");
        }
    }

    #[test]
    fn a_child_that_died_with_the_core_stopped_code_is_core_stopped() {
        let died = lost(LossCause::ChildDied, exited_with(CORE_STOPPED_EXIT_CODE));

        assert_eq!(reason_of_loss(&died), ReplaceReason::CoreStopped);
    }

    #[test]
    fn a_kill_of_the_worker_stays_a_kill_whatever_the_exit_code() {
        let killed = lost(
            LossCause::KilledByParent,
            exited_with(CORE_STOPPED_EXIT_CODE),
        );

        assert_eq!(reason_of_loss(&killed), ReplaceReason::KilledByParent);
    }
}

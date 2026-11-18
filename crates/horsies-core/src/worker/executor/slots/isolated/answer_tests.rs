//! The end of a task of an isolated slot whose child was lost, before and
//! after its body returned with work left on its loop (Python
//! `SlotHandle.loss_outcome`, `tests/unit/test_isolated_left_work.py`).

use tokio::sync::oneshot;

use super::{Answer, AttemptEnd, ChildExit, ChildLost, LossCause, ProcessId, TaskAnswer};
use crate::worker::executor::protocol::RunEnd;
use crate::worker::executor::slots::scripted::ok_envelope;
use crate::worker::executor::supervisor::{PartialFrame, ReapedExit};

fn lost(cause: LossCause, exit: ChildExit) -> ChildLost {
    ChildLost {
        cause,
        exit,
        partial: PartialFrame::NoneAtHand,
        pid: ProcessId::new(4242).unwrap(),
    }
}

/// The answer of a task in flight, not answered yet.
fn open_answer() -> TaskAnswer {
    let (sender, _receiver) = oneshot::channel();
    TaskAnswer::new(Answer::new(sender))
}

/// Python `TestHandle::test_a_loss_before_the_return_is_the_loss`: a child
/// lost before the body returned gives the crash path of the loss.
#[test]
fn a_loss_before_the_return_takes_the_crash_path_of_the_loss() {
    let killed = lost(
        LossCause::KilledByParent,
        ChildExit::Reaped(ReapedExit::KilledBySignal(9)),
    );

    let (_answer, end) = open_answer()
        .open_at_loss(&killed)
        .expect("the answer is still open");

    assert_eq!(end, AttemptEnd::of_lost_child(&killed));
}

/// Python `TestHandle::test_a_loss_after_the_return_keeps_what_the_body_returned`
/// (a slot defect, exit 3) and `TestDeadline::test_an_answered_loss_after_the_return_is_never_timed_out`:
/// after the body returned with work left, every loss of the child whose
/// exit is seen ends the task with what the body returned, never with the
/// crash path or a timeout.
#[test]
fn a_loss_after_the_return_keeps_what_the_body_returned() {
    let returned = AttemptEnd::Reported(RunEnd::Result(ok_envelope(7)));
    for loss in [
        lost(
            LossCause::SlotDefect,
            ChildExit::Reaped(ReapedExit::ExitedWith(3)),
        ),
        lost(
            LossCause::KilledByParent,
            ChildExit::Reaped(ReapedExit::KilledBySignal(9)),
        ),
        lost(
            LossCause::ChildDied,
            ChildExit::Reaped(ReapedExit::ExitedWith(3)),
        ),
        lost(
            LossCause::ProtocolViolation,
            ChildExit::Reaped(ReapedExit::ExitedWith(0)),
        ),
    ] {
        let mut answer = open_answer();
        answer.body_returned(returned.clone());

        let (_answer, end) = answer
            .open_at_loss(&loss)
            .expect("the answer is still open");

        assert_eq!(end, returned, "{loss}");
    }
}

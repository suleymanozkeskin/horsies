//! Slot defects: a failure in the slot's own path, not in its child.
//!
//! A defect costs the slot its child and the tasks on that child; the slot
//! then boots a new child. A slot that hits [`SLOT_DEFECT_LIMIT`] defects
//! with no finished task between them fails the pools, so a slot cannot
//! stop silently or spin while work waits for it.
//!
//! Defects:
//! - a parent frame that cannot be encoded;
//! - a panic of the slot task.
//!
//! A frame that cannot be written to the child is not a defect: it is a
//! fault of the child, and the child is lost.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::board::{PoolBoard, PoolFailure};
use super::replacement::ChildLeave;
use crate::worker::executor::protocol::FrameEncodeError;
use crate::worker::executor::supervisor::boot::BootFailureDetail;

/// Defects of one slot with no finished task between them that fail the
/// pools (Python `SLOT_DEFECT_LIMIT`).
pub const SLOT_DEFECT_LIMIT: u32 = 3;

/// What failed in a slot's own path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotDefect {
    pub detail: BootFailureDetail,
}

impl SlotDefect {
    /// A parent frame could not be encoded.
    pub fn frame_not_encoded(error: &FrameEncodeError) -> Self {
        Self {
            detail: BootFailureDetail::cut(&format!("a run frame could not be encoded: {error}")),
        }
    }

    /// The slot task panicked with `payload`.
    pub fn panicked(payload: &(dyn Any + Send)) -> Self {
        Self {
            detail: BootFailureDetail::cut(&format!(
                "the slot task panicked: {}",
                panic_text(payload)
            )),
        }
    }
}

/// The text of a panic payload: the message of `panic!`, or a note that the
/// payload is not text.
fn panic_text(payload: &(dyn Any + Send)) -> String {
    match (
        payload.downcast_ref::<&'static str>(),
        payload.downcast_ref::<String>(),
    ) {
        (Some(text), _) => (*text).to_owned(),
        (None, Some(text)) => text.clone(),
        (None, None) => "the panic payload is not text".to_owned(),
    }
}

/// Whether a task finished in one step of a slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TasksFinished {
    NoTask,
    AtLeastOne,
}

/// How one generation of a slot ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationEnd {
    /// The child retired, ended itself or was lost, or the pools stop or
    /// failed: `ChildLeave` says which.
    Ended(ChildLeave),
    /// A defect ended the generation; its child was killed.
    Defect(SlotDefect),
}

/// What one boot of a slot and the generation of its child gave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Served {
    Generation(GenerationEnd),
    /// The pools stop or failed during the boot.
    PoolEnded,
}

/// What the defect count says after a defect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefectCount {
    /// The slot boots a new child.
    BelowLimit,
    /// The slot hit [`SLOT_DEFECT_LIMIT`] defects in a row; `last` is the
    /// last one. The pools fail.
    Repeated { last: SlotDefect },
}

/// The defects of one slot since its last finished task (Python
/// `_Slot.defects`). It lives as long as the slot task, outside the code
/// that can hit a defect.
#[derive(Debug, Default)]
pub struct SlotDefects {
    in_a_row: u32,
}

impl SlotDefects {
    /// A task finished on the slot's child: the count starts again.
    pub fn task_finished(&mut self) {
        self.in_a_row = 0;
    }

    /// Note the tasks that one step of the slot finished.
    pub fn note(&mut self, finished: TasksFinished) {
        match finished {
            TasksFinished::AtLeastOne => self.task_finished(),
            TasksFinished::NoTask => {}
        }
    }

    /// Count one defect. Pure.
    pub fn defect(&mut self, defect: SlotDefect) -> DefectCount {
        self.in_a_row = self.in_a_row.saturating_add(1);
        match self.in_a_row >= SLOT_DEFECT_LIMIT {
            true => DefectCount::Repeated { last: defect },
            false => DefectCount::BelowLimit,
        }
    }

    pub fn in_a_row(&self) -> u32 {
        self.in_a_row
    }
}

/// What a slot does after a defect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AfterDefect {
    BootAgain,
    PoolFailed,
}

/// Log and count a defect of slot `index`. At the limit the pools fail with
/// [`PoolFailure::SlotDefectsRepeated`].
pub(super) fn count_defect(
    board: &PoolBoard,
    index: usize,
    defects: &mut SlotDefects,
    defect: SlotDefect,
) -> AfterDefect {
    tracing::error!(
        slot = index,
        in_a_row = defects.in_a_row().saturating_add(1),
        limit = SLOT_DEFECT_LIMIT,
        detail = %defect.detail,
        "a slot hit a defect; its child and its tasks are lost",
    );
    match defects.defect(defect) {
        DefectCount::BelowLimit => AfterDefect::BootAgain,
        DefectCount::Repeated { last } => {
            board.fail(PoolFailure::SlotDefectsRepeated {
                slot: index,
                detail: last.detail,
            });
            AfterDefect::PoolFailed
        }
    }
}

/// Poll a future and turn a panic inside a poll into `Err` with the panic
/// payload. The future is not polled again after a panic; the caller treats
/// the state it borrowed as that of a slot that hit a defect.
pub struct CatchPanic<F> {
    future: Pin<Box<F>>,
}

impl<F> CatchPanic<F> {
    pub fn new(future: F) -> Self {
        Self {
            future: Box::pin(future),
        }
    }
}

impl<F: Future> Future for CatchPanic<F> {
    type Output = Result<F::Output, Box<dyn Any + Send>>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self.future.as_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| future.poll(context))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::executor::slots::board::{ChildLayout, PoolHealth};

    fn defect(text: &str) -> SlotDefect {
        SlotDefect {
            detail: BootFailureDetail::cut(text),
        }
    }

    /// Python `TestSlotDefects::test_repeated_defects_fail_the_pool`, the
    /// count: the third defect in a row reaches the limit.
    #[test]
    fn the_third_defect_in_a_row_reaches_the_limit() {
        let mut defects = SlotDefects::default();

        let first = defects.defect(defect("one"));
        let second = defects.defect(defect("two"));
        let third = defects.defect(defect("three"));

        assert_eq!(first, DefectCount::BelowLimit);
        assert_eq!(second, DefectCount::BelowLimit);
        assert_eq!(
            third,
            DefectCount::Repeated {
                last: defect("three")
            }
        );
    }

    /// Python `TestSlotDefects::test_a_finished_task_clears_the_count`, the
    /// count: a finished task starts the count again.
    #[test]
    fn a_finished_task_clears_the_count() {
        let mut defects = SlotDefects::default();
        let _ = defects.defect(defect("one"));
        let _ = defects.defect(defect("two"));

        defects.note(TasksFinished::AtLeastOne);
        let third = defects.defect(defect("three"));

        assert_eq!(third, DefectCount::BelowLimit);
        assert_eq!(defects.in_a_row(), 1);
    }

    /// A step that finished no task leaves the count.
    #[test]
    fn a_step_with_no_finished_task_keeps_the_count() {
        let mut defects = SlotDefects::default();
        let _ = defects.defect(defect("one"));

        defects.note(TasksFinished::NoTask);

        assert_eq!(defects.in_a_row(), 1);
    }

    #[test]
    fn the_limit_fails_the_pools_with_the_slot_and_the_last_defect() {
        let board = PoolBoard::new(ChildLayout {
            isolated: 2,
            shared_async: 0,
            shared_thread: 0,
        });
        let mut defects = SlotDefects::default();

        let first = count_defect(&board, 1, &mut defects, defect("one"));
        let second = count_defect(&board, 1, &mut defects, defect("two"));
        assert_eq!(board.health(), PoolHealth::Serving);
        let third = count_defect(&board, 1, &mut defects, defect("three"));

        assert_eq!(first, AfterDefect::BootAgain);
        assert_eq!(second, AfterDefect::BootAgain);
        assert_eq!(third, AfterDefect::PoolFailed);
        assert_eq!(
            board.health(),
            PoolHealth::Failed(PoolFailure::SlotDefectsRepeated {
                slot: 1,
                detail: BootFailureDetail::cut("three"),
            })
        );
    }

    #[tokio::test]
    async fn a_panic_in_a_poll_is_an_err_with_its_payload() {
        let caught = CatchPanic::new(async {
            tokio::task::yield_now().await;
            panic!("the slot state is broken");
        })
        .await;

        let payload = caught.expect_err("the panic is caught");
        let defect = SlotDefect::panicked(payload.as_ref());
        assert_eq!(
            defect.detail.as_str(),
            "the slot task panicked: the slot state is broken"
        );
    }

    #[tokio::test]
    async fn a_future_that_ends_gives_its_output() {
        let caught = CatchPanic::new(async { 7_u8 }).await;

        assert_eq!(caught.ok(), Some(7));
    }

    #[test]
    fn a_formatted_panic_message_is_its_text() {
        let payload: Box<dyn Any + Send> = Box::new(format!("slot {} failed", 2));

        let defect = SlotDefect::panicked(payload.as_ref());

        assert_eq!(
            defect.detail.as_str(),
            "the slot task panicked: slot 2 failed"
        );
    }
}

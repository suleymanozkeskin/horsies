//! The seat body slot: the body form of a `SHARED_THREAD` task, which the
//! thread seat area fills before the serve.
//!
//! - Empty, a `SHARED_THREAD` task runs on the child's main thread: the
//!   blocking form of `body` sends the run to the JavaScript main thread and
//!   waits for its settle call. That form serves an isolated child of a
//!   worker that runs no thread child, and a thread child until the seats
//!   fill the slot.
//! - The thread seat area fills the slot with its own call, so it never
//!   changes the child host entry.

use std::sync::{Arc, OnceLock};

use horsies_core::core::task::fn_trait::BlockingTaskFn;

use crate::child::body::{ChildBodies, JsBlockingBody};

/// A body form of `SHARED_THREAD` tasks.
pub trait SeatBodyForm: Send + Sync {
    /// The blocking body of the task `task_name`.
    fn body(&self, task_name: &str, bodies: &Arc<ChildBodies>) -> Arc<dyn BlockingTaskFn>;
}

static SEAT_FORM: OnceLock<Arc<dyn SeatBodyForm>> = OnceLock::new();

/// The slot holds a form already.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeatSlotFilledAlready;

/// Put `form` in the slot, before the serve. A second fill is refused;
/// nothing changes then.
#[allow(
    dead_code,
    reason = "the thread seat area fills the slot with the body form of its seats"
)]
pub fn fill_seat_slot(form: Arc<dyn SeatBodyForm>) -> Result<(), SeatSlotFilledAlready> {
    SEAT_FORM
        .set(form)
        .map_err(|_filled_before| SeatSlotFilledAlready)
}

/// The blocking body of the `SHARED_THREAD` task `task_name`: the form in
/// the slot, or the main-thread form.
pub fn seat_body(task_name: &str, bodies: &Arc<ChildBodies>) -> Arc<dyn BlockingTaskFn> {
    match SEAT_FORM.get() {
        Some(form) => form.body(task_name, bodies),
        None => Arc::new(JsBlockingBody {
            task_name: task_name.to_owned(),
            bodies: Arc::clone(bodies),
        }),
    }
}

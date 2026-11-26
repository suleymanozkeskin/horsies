//! The handler child slot: the app of the handler child (its event
//! handlers, its start hooks, the spans of the attempts that the worker
//! ended), which the handler child area fills before the serve.
//!
//! - The serve takes the slot once. An empty slot gives the handler child no
//!   handler, no start hook and no span
//!   (`HandlerChildApp::without_handlers`); a task child reads none of it,
//!   and the worker starts no handler child for an app with no handler and
//!   no spans.
//! - The handler child area fills the slot with its own call, so it never
//!   changes the child host entry.

use std::sync::{Mutex, PoisonError};

use horsies_core::HandlerChildApp;

/// The state of the slot.
enum HandlerSlot {
    Empty,
    Filled(HandlerChildApp),
    /// The serve took it.
    Taken,
}

static SLOT: Mutex<HandlerSlot> = Mutex::new(HandlerSlot::Empty);

/// Why the slot took no app.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandlerSlotRefused {
    /// The slot holds an app already.
    FilledAlready,
    /// The serve took the slot: the child runs already.
    TakenByTheServe,
}

/// Put `app` in the slot, before the serve. Refused when the slot holds an
/// app or the serve took it; nothing changes then.
#[allow(
    dead_code,
    reason = "the handler child area fills the slot with its event handlers"
)]
pub fn fill_handler_slot(app: HandlerChildApp) -> Result<(), HandlerSlotRefused> {
    let mut slot = SLOT.lock().unwrap_or_else(PoisonError::into_inner);
    match *slot {
        HandlerSlot::Empty => {
            *slot = HandlerSlot::Filled(app);
            Ok(())
        }
        HandlerSlot::Filled(_) => Err(HandlerSlotRefused::FilledAlready),
        HandlerSlot::Taken => Err(HandlerSlotRefused::TakenByTheServe),
    }
}

/// The app of the handler child, for the serve: the app in the slot, or no
/// handler. The slot is taken: a later fill is refused. A second take is a
/// defect of the serve, which runs once per process.
pub fn take_handler_child() -> HandlerChildApp {
    let mut slot = SLOT.lock().unwrap_or_else(PoisonError::into_inner);
    match std::mem::replace(&mut *slot, HandlerSlot::Taken) {
        HandlerSlot::Empty => HandlerChildApp::without_handlers(),
        HandlerSlot::Filled(app) => app,
        HandlerSlot::Taken => panic!("the serve of the child took the handler child slot twice"),
    }
}

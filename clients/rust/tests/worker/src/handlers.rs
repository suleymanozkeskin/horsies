//! The event handlers of [`Instance::Handlers`]. Each handler records the
//! events it got in a marker file, which a test reads.

use horsies::events::{AttemptEnded, HandlerError};
use horsies::{Horsies, HorsiesError};
use uuid::Uuid;

use crate::instance::Instance;
use crate::markers;

/// The name of the handler of the `AttemptEnded` events.
pub const ATTEMPT_ENDED_HANDLER: &str = "e2e.attempt_ended";

/// The marker of the `AttemptEnded` handler: one line per event
/// ([`attempt_ended_line`]).
pub const ATTEMPT_ENDED_MARKER: &str = "handler-attempt-ended";

/// The line of the `AttemptEnded` event of `attempt` of task `task_id`.
pub fn attempt_ended_line(task_id: Uuid, attempt: u32) -> String {
    format!("{task_id} {attempt}")
}

/// Record an `AttemptEnded` event as a line of [`ATTEMPT_ENDED_MARKER`].
async fn record_attempt_ended(event: AttemptEnded) -> Result<(), HandlerError> {
    let line = attempt_ended_line(event.task_id, event.attempt.get());
    markers::append_line(ATTEMPT_ENDED_MARKER, &line).map_err(HandlerError::new)
}

/// Register the event handlers of `instance` on `app`. Stops at the first
/// refusal.
pub fn register(app: &mut Horsies, instance: Instance) -> Result<(), HorsiesError> {
    match instance {
        Instance::Handlers => {
            app.on_event::<AttemptEnded>(ATTEMPT_ENDED_HANDLER, record_attempt_ended)
        }
        Instance::Default
        | Instance::CustomQueues
        | Instance::ClusterCap
        | Instance::Recovery
        | Instance::SoftCap
        | Instance::RecoveryQueues
        | Instance::Scheduler => Ok(()),
    }
}

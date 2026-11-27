//! The JavaScript event handlers of a handler child, as the core's handlers.
//!
//! - One exhaustive `match` over the core's closed `EventTypeName`, one arm
//!   per event type, registers `EventHandlerSet::add_async::<E>`. A new event
//!   type of the core fails the build here until its arm exists.
//! - The core runs each handler on a thread of its own, with a queue of
//!   `HANDLER_QUEUE_EVENTS` events: a full queue drops the event for that
//!   handler and counts it. The handler's closure turns the event into its
//!   wire text (`WireEvent::of`) and calls the package's dispatcher with the
//!   handler's index and the text, through the callback primitive, and waits
//!   for its answer: one event at a time per handler, in the order of the
//!   worker.
//! - The package's dispatcher decodes the text with the generated decoder,
//!   runs the handler and answers `handled`, or `failed` with the cause of a
//!   throw, a rejection or a text that does not decode. A `failed` answer, a
//!   call that did not run and an event with no wire form are each a
//!   `HandlerError`: the core counts it in
//!   `horsies.telemetry.handler.errors` and logs it with the handler's name
//!   (the first, then one per `HANDLER_ERROR_LOG_EVERY`), and the handler
//!   takes its next event.

use std::sync::Arc;

use horsies_core::events::{
    AnyEvent, AttemptEnded, AttemptNotStarted, AttemptPlaced, AttemptRecorded, ChildReplaced,
    ChildStarted, ClaimReleased, EventTypeName, SlotDown, SlotRecovered, TaskClaimed,
    TaskRecovered, UnionEvent,
};
use horsies_core::frame_protocol::{WireEvent, MAX_EVENT_HANDLERS};
use horsies_core::{EventHandlerRefused, EventHandlerSet, HandlerError};
use napi_derive::napi;

use crate::call::callback::{CallbackFailure, CallbackWait, JsCallback, NotRunCause};

/// The calls of the dispatcher that wait for the JavaScript thread at once:
/// each handler waits for its call before it takes its next event, so at
/// most one call per handler waits.
pub const HANDLER_CALL_QUEUE_BOUND: usize = MAX_EVENT_HANDLERS;

/// The wait of a dispatcher call. A handler is user code of any duration; the
/// call ends at its answer or at the end of the environment.
const HANDLER_CALL_WAIT: CallbackWait = CallbackWait::NoBound;

/// One call of the dispatcher: the handler at `index` of the plan takes the
/// event of the wire text `wire_json` (`WireEvent::of`).
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandlerEventCall {
    pub index: u32,
    pub wire_json: String,
}

/// The answer of the dispatcher for one event.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandlerEventAnswer {
    /// The handler returned, or its Promise resolved.
    Handled,
    /// The handler threw or rejected, or the text did not decode: `cause`
    /// is the text that the core logs.
    Failed { cause: String },
}

/// The event dispatcher of the package.
pub type HandlerDispatcher =
    JsCallback<HandlerEventCall, HandlerEventAnswer, HANDLER_CALL_QUEUE_BOUND>;

/// One handler of the plan: its name and the wire tag of its event type.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandlerEntry {
    pub name: String,
    pub event_type: String,
}

/// Why the handlers of a plan are not the core's handlers: a defect of the
/// package, which checks each registration first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandlerPlanRefused {
    /// The plan names an event type that the core does not have.
    UnknownEventType { name: String, event_type: String },
    /// The core refused the registration.
    Refused(EventHandlerRefused),
}

impl std::fmt::Display for HandlerPlanRefused {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownEventType { name, event_type } => write!(
                formatter,
                "the handler {name} takes the event type {event_type}, which the core does not have"
            ),
            Self::Refused(refused) => write!(formatter, "{refused}"),
        }
    }
}

/// The core's event type of the wire tag `tag`, from the core's own list.
/// Pure.
fn event_type_of(tag: &str) -> Option<EventTypeName> {
    EventTypeName::ALL
        .iter()
        .copied()
        .find(|name| name.event_type().name == tag)
}

/// The core's handlers of `entries`, in order: handler `index` calls the
/// dispatcher with its index. Pure apart from the registration.
///
/// Errors: [`HandlerPlanRefused`] for the first entry that the core does
/// not take; nothing is returned then.
pub fn handler_set(
    entries: Vec<HandlerEntry>,
    dispatcher: &Arc<HandlerDispatcher>,
) -> Result<EventHandlerSet, HandlerPlanRefused> {
    let mut set = EventHandlerSet::new();
    for (index, entry) in entries.into_iter().enumerate() {
        let index = u32::try_from(index).unwrap_or_else(|_| {
            panic!("a plan holds at most {MAX_EVENT_HANDLERS} handlers, not {index}")
        });
        let Some(event_type) = event_type_of(&entry.event_type) else {
            return Err(HandlerPlanRefused::UnknownEventType {
                name: entry.name,
                event_type: entry.event_type,
            });
        };
        add_handler(&mut set, event_type, entry.name, index, dispatcher)
            .map_err(HandlerPlanRefused::Refused)?;
    }
    Ok(set)
}

/// Register the handler `index`, named `name`, for the events of
/// `event_type`: one arm per event type, no wildcard.
fn add_handler(
    set: &mut EventHandlerSet,
    event_type: EventTypeName,
    name: String,
    index: u32,
    dispatcher: &Arc<HandlerDispatcher>,
) -> Result<(), EventHandlerRefused> {
    macro_rules! add {
        ($event:ty) => {
            set.add_async::<$event>(name, delivery::<$event>(index, Arc::clone(dispatcher)))
        };
    }
    match event_type {
        EventTypeName::TaskClaimed => add!(TaskClaimed),
        EventTypeName::ClaimReleased => add!(ClaimReleased),
        EventTypeName::AttemptPlaced => add!(AttemptPlaced),
        EventTypeName::AttemptNotStarted => add!(AttemptNotStarted),
        EventTypeName::AttemptEnded => add!(AttemptEnded),
        EventTypeName::AttemptRecorded => add!(AttemptRecorded),
        EventTypeName::TaskRecovered => add!(TaskRecovered),
        EventTypeName::ChildStarted => add!(ChildStarted),
        EventTypeName::ChildReplaced => add!(ChildReplaced),
        EventTypeName::SlotDown => add!(SlotDown),
        EventTypeName::SlotRecovered => add!(SlotRecovered),
    }
}

/// The core's handler of the events of `E` for the handler `index`: the
/// wire text of each event to the dispatcher, and its answer back.
fn delivery<E>(
    index: u32,
    dispatcher: Arc<HandlerDispatcher>,
) -> impl AsyncFn(E) -> Result<(), HandlerError> + Send + Sync + 'static
where
    E: UnionEvent + Clone + Send + 'static,
{
    async move |event: E| -> Result<(), HandlerError> {
        let event: AnyEvent = event.into();
        let call = call_of(index, &event)?;
        answer_of(dispatcher.call(call, HANDLER_CALL_WAIT).await)
    }
}

/// The call of the handler `index` for `event`, or the failure of an event
/// with no wire form. Pure.
fn call_of(index: u32, event: &AnyEvent) -> Result<HandlerEventCall, HandlerError> {
    match WireEvent::of(event) {
        Ok(wire) => Ok(HandlerEventCall {
            index,
            wire_json: wire.as_str().to_owned(),
        }),
        Err(refused) => Err(HandlerError::new(format!(
            "the event has no wire form for JavaScript: {refused}"
        ))),
    }
}

/// The core's outcome of the dispatcher's answer. Pure.
fn answer_of(answered: Result<HandlerEventAnswer, CallbackFailure>) -> Result<(), HandlerError> {
    match answered {
        Ok(HandlerEventAnswer::Handled) => Ok(()),
        Ok(HandlerEventAnswer::Failed { cause }) => Err(HandlerError::new(cause)),
        Err(failure) => Err(HandlerError::new(call_failure_text(&failure))),
    }
}

/// The text of a dispatcher call that gave no answer. Pure.
fn call_failure_text(failure: &CallbackFailure) -> String {
    match failure {
        CallbackFailure::NotRun {
            cause: NotRunCause::QueueFull,
        } => "the handler did not run: the queue of the JavaScript thread is full".to_owned(),
        CallbackFailure::NotRun {
            cause: NotRunCause::EnvClosing,
        } => "the handler did not run: the JavaScript environment is closing".to_owned(),
        CallbackFailure::NoAnswer { bound } => {
            format!("the dispatcher gave no answer within {bound:?}")
        }
        CallbackFailure::Threw { name, message, .. } => {
            format!("the dispatcher threw {name}: {message}")
        }
        CallbackFailure::Panicked { cause } => {
            format!("the read of the dispatcher's answer panicked: {cause}")
        }
        CallbackFailure::AnswerRefused { cause } => {
            format!("the dispatcher's answer does not read: {cause}")
        }
        CallbackFailure::NodeApiRefused { status } => {
            format!("Node-API refused the dispatcher call: {status}")
        }
        CallbackFailure::OnJavaScriptThread => {
            "the handler was called on the JavaScript thread, which must run it".to_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use horsies_core::events::{Execution, SlotIndex};

    use super::*;

    #[test]
    fn each_event_type_of_the_core_has_its_wire_tag() {
        for name in EventTypeName::ALL {
            assert_eq!(event_type_of(name.event_type().name), Some(*name));
        }
        assert_eq!(event_type_of("AttemptFinished"), None);
    }

    #[test]
    fn the_call_of_an_event_holds_the_text_of_wire_event_of() {
        let event = AnyEvent::from(SlotRecovered {
            at: Utc
                .timestamp_opt(1_700_000_000, 0)
                .single()
                .expect("a valid instant"),
            child_execution: Execution::Isolated,
            slot: SlotIndex::try_from(2).expect("slot 2 is in range"),
        });
        let call = call_of(4, &event).expect("the event has a wire form");
        assert_eq!(call.index, 4);
        assert_eq!(
            call.wire_json,
            WireEvent::of(&event)
                .expect("the event has a wire form")
                .as_str()
        );
    }

    #[test]
    fn a_handled_answer_is_ok_and_a_failed_answer_is_a_handler_error() {
        assert_eq!(answer_of(Ok(HandlerEventAnswer::Handled)), Ok(()));
        assert_eq!(
            answer_of(Ok(HandlerEventAnswer::Failed {
                cause: "TypeError: boom".to_owned()
            })),
            Err(HandlerError::new("TypeError: boom"))
        );
    }

    #[test]
    fn each_call_failure_is_a_handler_error_that_names_its_cause() {
        for (failure, part) in [
            (
                CallbackFailure::NotRun {
                    cause: NotRunCause::QueueFull,
                },
                "queue",
            ),
            (
                CallbackFailure::NotRun {
                    cause: NotRunCause::EnvClosing,
                },
                "closing",
            ),
            (
                CallbackFailure::NoAnswer {
                    bound: std::time::Duration::from_secs(3),
                },
                "no answer within 3s",
            ),
            (
                CallbackFailure::Threw {
                    call_id: 1,
                    name: "TypeError".to_owned(),
                    message: "boom".to_owned(),
                },
                "TypeError: boom",
            ),
            (
                CallbackFailure::AnswerRefused {
                    cause: "no kind".to_owned(),
                },
                "does not read",
            ),
            (CallbackFailure::OnJavaScriptThread, "JavaScript thread"),
        ] {
            let error = answer_of(Err(failure)).expect_err("a call failure is a handler error");
            assert!(error.cause().contains(part), "{}", error.cause());
        }
    }
}

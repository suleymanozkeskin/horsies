//! The public lifecycle events of the core worker: one Rust type per event,
//! their wire form, and the data that `horsies-codegen` renders into the
//! Python classes and their decoder (`horsies/_catalog/events.py`).
//!
//! Rules of the contract:
//!
//! - An event holds ids, names, codes, numbers and times. It never holds task
//!   input, a result, a failed reason or an exception text.
//! - Every field has a closed type or a bounded one ([`ValueForm`]). Units are
//!   in the types: a duration is a [`std::time::Duration`], a time is a
//!   `DateTime<Utc>`. A state that can be missing or unknown is a named
//!   variant.
//! - A handler subscribes to one event type, never to the union
//!   [`AnyEvent`]. A later contract version can add an event type; that
//!   changes no handler of another type. Delivery is best effort: at most
//!   once, and a drop is counted, never retried. The history tables are the
//!   record for audit.
//! - The events of one task are in the order that the core emitted them. No
//!   order holds across tasks.
//!
//! Wire form: an event is a JSON array `[tag, field, ...]`. The tag is the
//! event type's name; the fields follow in the order of the type. A choice
//! is its value's text; a value of a variant type is `[variant tag, field,
//! ...]`. [`encode_event`] writes it; [`decode_event`] reads it and refuses
//! an unknown tag ([`EventDecodeError::UnknownEventType`]) and a malformed
//! event with a named error.
//!
//! Versions: [`EVENT_CONTRACT_VERSION`] names the content of the contract.
//! Any change of an event type, a field or a closed type changes the
//! version, and the snapshot of the new version
//! (`tests/fixtures/event_contract/v<version>.txt`) is added. Tags are names,
//! never positions, so a new type changes no existing tag.

mod records;
mod schema;
mod values;

#[cfg(test)]
mod tests;

pub use records::{
    AttemptEnded, AttemptNotStarted, AttemptPlaced, AttemptRecorded, ChildReplaced, ChildStarted,
    ClaimReleased, SlotDown, SlotRecovered, TaskClaimed, TaskRecovered,
};
pub use schema::{
    wire_text, wire_variant, ChoiceValue, CodeUnion, DecodeCause, DecodeRefusal, EncodeCause,
    EncodeRefusal, EventField, EventNamedValue, EventRecord, EventType, EventValue, FieldAccess,
    JsonKind, NamedShape, NamedType, RefusedName, ValueForm, VariantType, WirePath, WireValue,
    MAX_REFUSED_NAME_CHARS,
};
pub use values::{
    AttemptEnd, AttemptErrorCode, AttemptNumber, AttemptRecord, AttemptStart, ChildResult,
    CustomCodeText, MaxRetries, OperatorCancelEnd, Priority, QueueName, RetriesLeft, RetryCount,
    RunTrace, SlotDownCause, SlotIndex, TaskName, TraceCarrierText, WorkerDecision, WorkerIdText,
    WorkflowPlace, BUILT_IN_TASK_CODES, MAX_ATTEMPT, MAX_EVENT_CUSTOM_CODE_CHARS,
    MAX_EVENT_QUEUE_NAME_CHARS, MAX_EVENT_TASK_NAME_CHARS, MAX_EVENT_TRACE_CARRIER_CHARS,
    MAX_EVENT_WORKER_ID_CHARS, MAX_RETRY_COUNT, MAX_SLOT_INDEX, MAX_TASK_RETRIES,
};

// The closed types of other modules that event fields hold: the core's own
// types, and the label sets of the metrics that event fields share.
pub use crate::core::boot_failure::ShortageErrno;
pub use crate::core::task::Execution;
pub use crate::telemetry::labels::{RecoveryAction, ReleaseReason, ReplaceReason};
pub use crate::worker::executor::protocol::StopReason;

/// The version of the event contract: its event types, their fields and the
/// closed types that the fields hold.
pub const EVENT_CONTRACT_VERSION: u32 = 9;

/// The event contract, as `horsies-codegen` renders it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventContract {
    pub version: u32,
    /// The event types, in the order of [`AnyEvent`].
    pub events: &'static [EventType],
    /// The closed types that event fields hold, in render order: a type
    /// comes after the types that its variants' fields name.
    pub value_types: &'static [NamedType],
}

/// The contract of this core.
pub const EVENT_CONTRACT: EventContract = EventContract {
    version: EVENT_CONTRACT_VERSION,
    events: EVENT_TYPES,
    value_types: EVENT_VALUE_TYPES,
};

/// Every closed type that an event field holds, in render order.
pub const EVENT_VALUE_TYPES: &[NamedType] = &[
    <Execution as EventNamedValue>::NAMED,
    <StopReason as EventNamedValue>::NAMED,
    <ShortageErrno as EventNamedValue>::NAMED,
    <ReleaseReason as EventNamedValue>::NAMED,
    <AttemptErrorCode as EventNamedValue>::NAMED,
    <OperatorCancelEnd as EventNamedValue>::NAMED,
    <ChildResult as EventNamedValue>::NAMED,
    <AttemptStart as EventNamedValue>::NAMED,
    <WorkerDecision as EventNamedValue>::NAMED,
    <RunTrace as EventNamedValue>::NAMED,
    <WorkflowPlace as EventNamedValue>::NAMED,
    <AttemptEnd as EventNamedValue>::NAMED,
    <AttemptRecord as EventNamedValue>::NAMED,
    <RecoveryAction as EventNamedValue>::NAMED,
    <ReplaceReason as EventNamedValue>::NAMED,
    <SlotDownCause as EventNamedValue>::NAMED,
];

/// An event that does not decode.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EventDecodeError {
    /// The value is not a JSON array whose first item is a string.
    #[error("the value is not an event: a JSON array whose first item is the event type's name")]
    NotAnEvent,
    /// The first item names no event type of this contract version.
    #[error("no event type of this contract version is named '{tag}'")]
    UnknownEventType { tag: RefusedName },
    /// The items after the tag do not match the fields of the type.
    #[error("the {event} event does not decode at {refusal}")]
    Malformed {
        event: &'static str,
        refusal: DecodeRefusal,
    },
}

/// An event that has no wire form: a duration or a time outside the range of
/// its wire number.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the {event} event does not encode at {refusal}")]
pub struct EventEncodeError {
    pub event: &'static str,
    pub refusal: EncodeRefusal,
}

/// An event type of the union [`AnyEvent`], with its name. A point of the
/// worker builds one of these; the worker builds it only when its handler
/// child takes events of its type. A handler of a Rust handler child takes
/// the events of one of these types.
pub trait UnionEvent: EventRecord + Into<AnyEvent> {
    /// The name of the type: its wire tag.
    const NAME: EventTypeName;

    /// The record of `event` when it is an event of this type; `None` for
    /// an event of any other type. Pure.
    fn of_event(event: &AnyEvent) -> Option<&Self>;
}

/// Defines the union [`AnyEvent`], the names [`EventTypeName`], the list
/// [`EVENT_TYPES`] and the dispatch of the codec from one list of event
/// types, so that no event type is in one of them and not in the others.
macro_rules! event_union {
    ($($event:ident,)+) => {
        /// One event of the contract. A handler subscribes to one event
        /// type, never to this union: a later contract version can add a
        /// variant.
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub enum AnyEvent {
            $($event($event),)+
        }

        /// The name of one event type: the tag of its wire form. A handler
        /// takes the events of one type, which it names with this value.
        /// The worker manifest names the type of each handler with it: its
        /// JSON form is the name.
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            serde::Serialize,
            serde::Deserialize,
        )]
        pub enum EventTypeName {
            $($event,)+
        }

        /// A Python `Literal` of the names, with the doc of each type.
        impl crate::manifest_schema::ManifestValue for EventTypeName {
            const PYTHON: crate::manifest_schema::PythonValue =
                crate::manifest_schema::PythonValue::Named("EventTypeName");
        }

        impl crate::manifest_schema::ManifestItem for EventTypeName {
            const ITEM: crate::manifest_schema::ManifestType = crate::manifest_schema::ManifestType {
                python_name: "EventTypeName",
                doc: "The name of one event type: the tag of its wire form. A handler takes the \
                      events of one type.",
                shape: crate::manifest_schema::ManifestShape::Choice {
                    values: &[$(
                        crate::manifest_schema::ChoiceValue {
                            value: stringify!($event),
                            doc: <$event as EventRecord>::TYPE.doc,
                        },
                    )+],
                },
            };
        }

        impl EventTypeName {
            /// Every event type name, in the order of [`AnyEvent`].
            pub const ALL: &'static [EventTypeName] = &[$(Self::$event,)+];

            /// The event type of this name.
            pub fn event_type(self) -> EventType {
                match self {
                    $(Self::$event => <$event as EventRecord>::TYPE,)+
                }
            }
        }

        $(
            impl UnionEvent for $event {
                const NAME: EventTypeName = EventTypeName::$event;

                fn of_event(event: &AnyEvent) -> Option<&Self> {
                    match event {
                        AnyEvent::$event(record) => Some(record),
                        // Every other event type of the union.
                        _ => None,
                    }
                }
            }
        )+

        /// The event types, in the order of [`AnyEvent`]. A client checks a
        /// handler's event type against this list.
        pub const EVENT_TYPES: &[EventType] = &[$(<$event as EventRecord>::TYPE,)+];

        impl AnyEvent {
            /// The type of this event.
            pub fn event_type(&self) -> EventType {
                match self {
                    $(Self::$event(_) => <$event as EventRecord>::TYPE,)+
                }
            }

            /// The name of the type of this event.
            pub fn type_name(&self) -> EventTypeName {
                match self {
                    $(Self::$event(_) => EventTypeName::$event,)+
                }
            }
        }

        $(
            impl From<$event> for AnyEvent {
                fn from(event: $event) -> Self {
                    Self::$event(event)
                }
            }
        )+

        fn encode_union(event: &AnyEvent) -> Result<WireValue, EventEncodeError> {
            match event {
                $(AnyEvent::$event(record) => encode_record(record),)+
            }
        }

        fn decode_union(tag: &str, fields: &[WireValue]) -> Result<AnyEvent, EventDecodeError> {
            $(
                if tag == <$event as EventRecord>::TYPE.name {
                    return decode_record::<$event>(fields).map(AnyEvent::$event);
                }
            )+
            Err(EventDecodeError::UnknownEventType {
                tag: RefusedName::cut(tag),
            })
        }
    };
}

event_union!(
    TaskClaimed,
    ClaimReleased,
    AttemptPlaced,
    AttemptNotStarted,
    AttemptEnded,
    AttemptRecorded,
    TaskRecovered,
    ChildStarted,
    ChildReplaced,
    SlotDown,
    SlotRecovered,
);

/// The wire form of `event`: `[tag, field, ...]`.
///
/// Errors: [`EventEncodeError`] names the event type and the field whose
/// duration or time the wire cannot hold. Pure.
pub fn encode_event(event: &AnyEvent) -> Result<WireValue, EventEncodeError> {
    encode_union(event)
}

/// The event of a wire value: the JSON value of one event, as a parser of
/// the frame body gives it.
///
/// Errors: [`EventDecodeError::NotAnEvent`],
/// [`EventDecodeError::UnknownEventType`], or
/// [`EventDecodeError::Malformed`] with the path to the first value that
/// does not decode. A refusal of one event leaves the other events of a
/// batch to their own decode. Pure.
pub fn decode_event(wire: &WireValue) -> Result<AnyEvent, EventDecodeError> {
    let (tag, fields) = match wire.as_array().and_then(|items| items.split_first()) {
        Some((WireValue::String(tag), fields)) => (tag.as_str(), fields),
        Some(_) | None => return Err(EventDecodeError::NotAnEvent),
    };
    decode_union(tag, fields)
}

fn encode_record<E: EventRecord>(record: &E) -> Result<WireValue, EventEncodeError> {
    let mut items = Vec::with_capacity(E::TYPE.fields.len() + 1);
    items.push(WireValue::String(E::TYPE.name.to_owned()));
    record
        .fields_to_wire(&mut items)
        .map_err(|refusal| EventEncodeError {
            event: E::TYPE.name,
            refusal,
        })?;
    Ok(WireValue::Array(items))
}

fn decode_record<E: EventRecord>(fields: &[WireValue]) -> Result<E, EventDecodeError> {
    E::from_wire_fields(fields).map_err(|refusal| EventDecodeError::Malformed {
        event: E::TYPE.name,
        refusal,
    })
}

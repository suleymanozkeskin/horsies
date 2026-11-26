//! The events of an `events` frame: each event in its wire form
//! ([`encode_event`]), and the batch that one frame carries as its body.
//!
//! The frame checks the size of each event and the number of events. It does
//! not read an event: the handler child decodes each one on its own
//! ([`crate::events::decode_event`]), so a refusal of one event leaves the
//! other events of the batch to their own decode. Pure: no I/O.

use std::ops::Range;

use serde_json::value::RawValue;

use super::bounds::{MAX_EVENTS_PER_FRAME, MAX_EVENT_WIRE_BYTES};
use super::metadata::{text_body, MalformedFrame};
use crate::core::codec::{check_json_text, to_json_bytes};
use crate::events::{encode_event, AnyEvent, EventEncodeError};

/// One event in its wire form: the compact JSON text of [`encode_event`],
/// at most [`MAX_EVENT_WIRE_BYTES`] bytes. Only [`WireEvent::of`] builds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireEvent(String);

/// Why an event has no place in an `events` frame. The writer drops the
/// event and counts it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WireEventRefusal {
    /// The event has no wire form: a duration or a time outside the range of
    /// its wire number.
    #[error(transparent)]
    NotEncoded(EventEncodeError),
    /// The wire form of the event is over its bound.
    #[error(
        "the {event} event has {bytes} bytes of wire form; the bound is {max}",
        max = MAX_EVENT_WIRE_BYTES
    )]
    OverBound { event: &'static str, bytes: usize },
}

impl WireEvent {
    /// The wire form of `event`. Pure.
    ///
    /// Errors: [`WireEventRefusal::NotEncoded`] for an event with no wire
    /// form; [`WireEventRefusal::OverBound`] for a wire form over
    /// [`MAX_EVENT_WIRE_BYTES`].
    pub fn of(event: &AnyEvent) -> Result<WireEvent, WireEventRefusal> {
        let wire = encode_event(event).map_err(WireEventRefusal::NotEncoded)?;
        let bytes = to_json_bytes(&wire).expect(
            "the wire value of an event holds only texts, whole numbers and arrays, so its \
             encoding cannot fail",
        );
        let text = String::from_utf8(bytes).expect("the JSON encoder writes UTF-8");
        match text.len() <= MAX_EVENT_WIRE_BYTES {
            true => Ok(WireEvent(text)),
            false => Err(WireEventRefusal::OverBound {
                event: event.event_type().name,
                bytes: text.len(),
            }),
        }
    }

    /// The JSON text of the event.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The events of one `events` frame, in the order that the core emitted
/// them: 1 to [`MAX_EVENTS_PER_FRAME`] events of at most
/// [`MAX_EVENT_WIRE_BYTES`] bytes each. The body of the frame is the JSON
/// array of their wire forms. Only [`EventBatch::new`] and the frame decoder
/// build it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBatch {
    /// The body of the frame: `[event,event,...]`.
    body: String,
    /// The byte range of each event in `body`, in order.
    events: Vec<Range<usize>>,
}

/// A batch of events with no event, or with more than
/// [`MAX_EVENTS_PER_FRAME`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "an events frame carries 1 to {max} events, not {count}",
    max = MAX_EVENTS_PER_FRAME
)]
pub struct EventCountOutOfBounds {
    pub count: usize,
}

impl EventBatch {
    /// The batch of `events`, in their order. Pure.
    ///
    /// Errors: [`EventCountOutOfBounds`] for no event or more than
    /// [`MAX_EVENTS_PER_FRAME`].
    pub fn new(events: &[WireEvent]) -> Result<EventBatch, EventCountOutOfBounds> {
        match (1..=MAX_EVENTS_PER_FRAME).contains(&events.len()) {
            true => {}
            false => {
                return Err(EventCountOutOfBounds {
                    count: events.len(),
                })
            }
        }
        let text_bytes: usize = events.iter().map(|event| event.0.len() + 1).sum();
        let mut body = String::with_capacity(text_bytes + 1);
        let mut ranges = Vec::with_capacity(events.len());
        body.push('[');
        for (index, event) in events.iter().enumerate() {
            match index {
                0 => {}
                _ => body.push(','),
            }
            let start = body.len();
            body.push_str(&event.0);
            ranges.push(start..body.len());
        }
        body.push(']');
        Ok(EventBatch {
            body,
            events: ranges,
        })
    }

    /// The JSON text of each event, in order.
    pub fn events(&self) -> impl ExactSizeIterator<Item = &str> + '_ {
        self.events.iter().map(|range| &self.body[range.clone()])
    }

    /// The body of the frame: the JSON array of the events.
    pub(super) fn body(&self) -> &str {
        &self.body
    }
}

/// The batch of an `events` frame from its body. Pure.
///
/// Errors: [`MalformedFrame::BodyNotUtf8`],
/// [`MalformedFrame::EventsBodyNotJson`] for a body that is not strict JSON,
/// [`MalformedFrame::EventsBodyNotArray`], [`MalformedFrame::EventCount`]
/// for no event or more than [`MAX_EVENTS_PER_FRAME`], and
/// [`MalformedFrame::EventOverBound`] for an event over
/// [`MAX_EVENT_WIRE_BYTES`].
pub(super) fn decode_event_batch(body: Vec<u8>) -> Result<EventBatch, MalformedFrame> {
    let text = text_body(body)?;
    check_json_text(&text).map_err(MalformedFrame::EventsBodyNotJson)?;
    let events = event_ranges(&text)?;
    Ok(EventBatch { body: text, events })
}

/// The byte range of each event of a strict JSON body in `text`, checked
/// against the bounds of the batch.
fn event_ranges(text: &str) -> Result<Vec<Range<usize>>, MalformedFrame> {
    let items: Vec<&RawValue> =
        serde_json::from_str(text).map_err(|_| MalformedFrame::EventsBodyNotArray)?;
    match (1..=MAX_EVENTS_PER_FRAME).contains(&items.len()) {
        true => {}
        false => return Err(MalformedFrame::EventCount { count: items.len() }),
    }
    let body_start = text.as_ptr() as usize;
    let mut ranges = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let event = item.get();
        match event.len() <= MAX_EVENT_WIRE_BYTES {
            true => {}
            false => {
                return Err(MalformedFrame::EventOverBound {
                    index,
                    bytes: event.len(),
                })
            }
        }
        // A borrowed raw value is a slice of `text`, so its offset is in it.
        let start = event.as_ptr() as usize - body_start;
        ranges.push(start..start + event.len());
    }
    Ok(ranges)
}

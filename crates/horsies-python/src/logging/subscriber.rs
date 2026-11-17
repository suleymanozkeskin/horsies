//! The `tracing` subscriber of the core: events only, no span tracking.
//!
//! It enables an event when the target has a route and the level snapshot
//! lets the level through, reads the `message` field as the record's message
//! and every other field as a [`RecordField`], and pushes the record into
//! the sink. Spans are never enabled, so span context is not forwarded.

use std::fmt::{self, Write as _};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use tracing::field::{Field, Visit};
use tracing::span;
use tracing::subscriber::Interest;
use tracing::{Event, Metadata, Subscriber};

use super::levels::RecordLevel;
use super::names::route;
use super::{
    current_thread_name, CoreLogRecord, FieldValue, LogSink, RecordField, RecordSource, CUT_MARK,
    MAX_RECORD_FIELDS, MAX_RECORD_TEXT_BYTES, VALUE_CUT_MARK,
};

/// The name of the field that holds the text of an event.
const MESSAGE_FIELD: &str = "message";

/// The subscriber that feeds the log sink.
pub struct CoreSubscriber {
    sink: Arc<LogSink>,
    next_span_id: AtomicU64,
}

impl CoreSubscriber {
    pub fn new(sink: Arc<LogSink>) -> Self {
        Self {
            sink,
            next_span_id: AtomicU64::new(1),
        }
    }
}

impl Subscriber for CoreSubscriber {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        // The level snapshot changes at run time, so a routed event callsite
        // is asked again at each event.
        match (metadata.is_event(), route(metadata.target())) {
            (true, Some(_)) => Interest::sometimes(),
            (true, None) | (false, _) => Interest::never(),
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        match (metadata.is_event(), route(metadata.target())) {
            (true, Some(logger)) => self
                .sink
                .enabled(logger, RecordLevel::from_tracing(*metadata.level())),
            (true, None) | (false, _) => false,
        }
    }

    fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
        // Spans are never enabled, so this runs only if another layer of
        // `tracing` forces a span through. The id only has to be non-zero.
        let next = self.next_span_id.fetch_add(1, Ordering::Relaxed);
        span::Id::from_non_zero_u64(NonZeroU64::new(next).unwrap_or(NonZeroU64::MIN))
    }

    fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

    fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        let metadata = event.metadata();
        let Some(logger) = route(metadata.target()) else {
            return;
        };
        let mut read = EventFields::new();
        event.record(&mut read);
        let source = match (metadata.file(), metadata.line()) {
            (Some(file), Some(line)) => RecordSource::Known { file, line },
            (None, _) | (_, None) => RecordSource::Unknown,
        };
        self.sink.push(CoreLogRecord {
            logger,
            level: RecordLevel::from_tracing(*metadata.level()),
            message: read.message,
            fields: read.fields,
            source,
            created: SystemTime::now(),
            thread_name: current_thread_name(),
        });
    }

    fn enter(&self, _span: &span::Id) {}

    fn exit(&self, _span: &span::Id) {}
}

/// The fields of one event.
///
/// - The `message` field, of any type, is the text of the record's message.
/// - Every other field is a [`RecordField`]: a bool, an integer or a float
///   keeps its type; any other value is its text.
/// - The message and the text values together hold at most
///   [`MAX_RECORD_TEXT_BYTES`], in the order that the event records them
///   (the macros of `tracing` record the message first). A text that
///   reaches the bound is cut at a character boundary and ends with
///   [`CUT_MARK`] (the message) or [`VALUE_CUT_MARK`] (a value); a text value
///   after the bound is the mark alone. Formatting stops at the bound.
/// - At most [`MAX_RECORD_FIELDS`] fields are kept: the first ones that the
///   event records.
struct EventFields {
    message: String,
    fields: Vec<RecordField>,
    /// Bytes of text that the record can still take.
    text_room: usize,
}

impl EventFields {
    fn new() -> Self {
        Self {
            message: String::new(),
            fields: Vec::new(),
            text_room: MAX_RECORD_TEXT_BYTES,
        }
    }

    /// Keep a bool, an integer or a float with its type, or its text when
    /// it is the message.
    fn keep_typed(&mut self, field: &Field, value: FieldValue) {
        match field.name() {
            MESSAGE_FIELD => self.keep_text(field, format_args!("{value}")),
            name if self.fields.len() < MAX_RECORD_FIELDS => {
                self.fields.push(RecordField { name, value });
            }
            _past_the_field_bound => {}
        }
    }

    /// Keep the text of a value, as the message or as a text value.
    fn keep_text(&mut self, field: &Field, value: fmt::Arguments<'_>) {
        match field.name() {
            MESSAGE_FIELD => write_bounded(&mut self.message, &mut self.text_room, value, CUT_MARK),
            name if self.fields.len() < MAX_RECORD_FIELDS => {
                let mut text = String::new();
                write_bounded(&mut text, &mut self.text_room, value, VALUE_CUT_MARK);
                self.fields.push(RecordField {
                    name,
                    value: FieldValue::Text(text),
                });
            }
            _past_the_field_bound => {}
        }
    }
}

impl Visit for EventFields {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.keep_typed(field, FieldValue::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.keep_typed(field, FieldValue::Signed(i128::from(value)));
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.keep_typed(field, FieldValue::Signed(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.keep_typed(field, FieldValue::Unsigned(u128::from(value)));
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.keep_typed(field, FieldValue::Unsigned(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.keep_typed(field, FieldValue::Float(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.keep_text(field, format_args!("{value}"));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.keep_text(field, format_args!("{value}"));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.keep_text(field, format_args!("{value:?}"));
    }
}

/// Append `value` to `text`, at most `room` bytes of it, and take the bytes
/// written from `room`. A value that does not fit is cut at a character
/// boundary, its formatting stops, `room` becomes 0, and `mark` ends the
/// text.
fn write_bounded(text: &mut String, room: &mut usize, value: fmt::Arguments<'_>, mark: &str) {
    let mut bounded = BoundedText {
        text: &mut *text,
        room: *room,
        cut: false,
    };
    // A cut ends the formatting with `fmt::Error`; `cut` records it.
    let _formatting_stopped = bounded.write_fmt(value);
    let BoundedText {
        room: left, cut, ..
    } = bounded;
    *room = left;
    if cut {
        text.push_str(mark);
    }
}

/// A text that takes at most `room` more bytes.
struct BoundedText<'a> {
    text: &'a mut String,
    room: usize,
    /// A write did not fit: the text holds its part up to a character
    /// boundary, and the room is spent.
    cut: bool,
}

impl fmt::Write for BoundedText<'_> {
    fn write_str(&mut self, part: &str) -> fmt::Result {
        if part.len() <= self.room {
            self.text.push_str(part);
            self.room -= part.len();
            return Ok(());
        }
        self.text
            .push_str(&part[..char_boundary_at_or_below(part, self.room)]);
        self.room = 0;
        self.cut = true;
        Err(fmt::Error)
    }
}

/// The largest character boundary of `text` at or below `index`, which is
/// at most the length of `text`. A UTF-8 character is at most 4 bytes, so
/// the boundary is at most 3 bytes below `index`.
fn char_boundary_at_or_below(text: &str, index: usize) -> usize {
    (index.saturating_sub(3)..=index)
        .rev()
        .find(|position| text.is_char_boundary(*position))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::LogBridge;

    /// The record of the one event that `log` writes through a new bridge.
    fn record_of(log: impl FnOnce()) -> CoreLogRecord {
        let (bridge, receiver) = LogBridge::new();
        tracing::dispatcher::with_default(bridge.dispatch(), log);
        receiver.try_recv().expect("the event reached the channel")
    }

    fn text_field(name: &'static str, text: &str) -> RecordField {
        RecordField {
            name,
            value: FieldValue::Text(text.to_owned()),
        }
    }

    #[test]
    fn the_message_is_the_text_alone_and_each_other_field_keeps_its_type() {
        let record = record_of(|| {
            tracing::warn!(
                target: "horsies.monitoring",
                read = "schedule state",
                bound = 10_000_usize,
                signed = -3_i64,
                ratio = 0.5_f64,
                retried = true,
                cause = %"connection refused",
                shape = ?Some(1),
                "schedule state: the list is cut at {} schedules",
                10_000
            );
        });

        assert_eq!(
            record.message,
            "schedule state: the list is cut at 10000 schedules"
        );
        assert_eq!(
            record.fields,
            vec![
                text_field("read", "schedule state"),
                RecordField {
                    name: "bound",
                    value: FieldValue::Unsigned(10_000),
                },
                RecordField {
                    name: "signed",
                    value: FieldValue::Signed(-3),
                },
                RecordField {
                    name: "ratio",
                    value: FieldValue::Float(0.5),
                },
                RecordField {
                    name: "retried",
                    value: FieldValue::Bool(true),
                },
                text_field("cause", "connection refused"),
                text_field("shape", "Some(1)"),
            ]
        );
    }

    #[test]
    fn an_event_with_no_other_field_has_no_fields() {
        let record = record_of(|| {
            tracing::warn!(target: "horsies.monitoring", "the listener reconnected");
        });

        assert_eq!(record.message, "the listener reconnected");
        assert_eq!(record.fields, Vec::new());
    }

    #[test]
    fn a_message_past_the_text_bound_is_cut_at_a_character_boundary() {
        // `€` is 3 bytes and the bound is not a multiple of 3, so the bound
        // falls inside a character.
        let long = "€".repeat(MAX_RECORD_TEXT_BYTES);
        let record = record_of(|| {
            tracing::warn!(target: "horsies.monitoring", detail = "late", "{long}");
        });

        assert_eq!(
            record.message,
            format!("{}{CUT_MARK}", "€".repeat(MAX_RECORD_TEXT_BYTES / 3))
        );
        assert_eq!(
            record.fields,
            vec![text_field("detail", VALUE_CUT_MARK)],
            "a text value after the bound is the mark alone",
        );
    }

    #[test]
    fn a_value_that_reaches_the_text_bound_is_cut_and_a_later_number_is_kept() {
        let message = "m".repeat(MAX_RECORD_TEXT_BYTES - 4);
        let record = record_of(|| {
            tracing::warn!(
                target: "horsies.monitoring",
                detail = "abcdefgh",
                count = 3_u64,
                "{message}"
            );
        });

        assert_eq!(record.message, message);
        assert_eq!(
            record.fields,
            vec![
                text_field("detail", &format!("abcd{VALUE_CUT_MARK}")),
                RecordField {
                    name: "count",
                    value: FieldValue::Unsigned(3),
                },
            ]
        );
    }

    #[test]
    fn fields_past_the_field_bound_are_left_out() {
        let record = record_of(|| {
            tracing::warn!(
                target: "horsies.monitoring",
                f00 = 0_u64, f01 = 1_u64, f02 = 2_u64, f03 = 3_u64, f04 = 4_u64,
                f05 = 5_u64, f06 = 6_u64, f07 = 7_u64, f08 = 8_u64, f09 = 9_u64,
                f10 = 10_u64, f11 = 11_u64, f12 = 12_u64, f13 = 13_u64, f14 = 14_u64,
                f15 = 15_u64, f16 = 16_u64, f17 = 17_u64, f18 = 18_u64, f19 = 19_u64,
                f20 = 20_u64, f21 = 21_u64, f22 = 22_u64, f23 = 23_u64, f24 = 24_u64,
                f25 = 25_u64, f26 = 26_u64, f27 = 27_u64, f28 = 28_u64, f29 = 29_u64,
                f30 = 30_u64, f31 = 31_u64, f32 = 32_u64,
                "many fields"
            );
        });

        assert_eq!(record.fields.len(), MAX_RECORD_FIELDS);
        assert_eq!(
            record.fields.last(),
            Some(&RecordField {
                name: "f31",
                value: FieldValue::Unsigned(31),
            })
        );
    }

    #[test]
    fn a_typed_message_field_is_the_text_of_its_value() {
        let record = record_of(|| {
            tracing::warn!(target: "horsies.monitoring", message = 42_u64);
        });

        assert_eq!(record.message, "42");
        assert_eq!(record.fields, Vec::new());
    }
}

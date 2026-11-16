//! The `tracing` subscriber of the core: events only, no span tracking.
//!
//! It enables an event when the target has a route and the level snapshot
//! lets the level through, formats the fields into one message, and pushes
//! the record into the sink. Spans are never enabled, so span context is not
//! forwarded.

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
    current_thread_name, cut_to_limit, CoreLogRecord, LogSink, RecordSource,
    MAX_RECORD_MESSAGE_BYTES,
};

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
        let mut fields = FieldText::default();
        event.record(&mut fields);
        let source = match (metadata.file(), metadata.line()) {
            (Some(file), Some(line)) => RecordSource::Known { file, line },
            (None, _) | (_, None) => RecordSource::Unknown,
        };
        self.sink.push(CoreLogRecord {
            logger,
            level: RecordLevel::from_tracing(*metadata.level()),
            message: fields.into_message(),
            source,
            created: SystemTime::now(),
            thread_name: current_thread_name(),
        });
    }

    fn enter(&self, _span: &span::Id) {}

    fn exit(&self, _span: &span::Id) {}
}

/// The fields of one event as text: the `message` field first, then
/// ` name=value` for every other field. Bounded by
/// [`MAX_RECORD_MESSAGE_BYTES`].
#[derive(Default)]
struct FieldText {
    message: String,
    fields: String,
}

impl FieldText {
    fn append(&mut self, field: &Field, value: fmt::Arguments<'_>) {
        if self.message.len() + self.fields.len() >= MAX_RECORD_MESSAGE_BYTES {
            return;
        }
        // Writing into a `String` cannot fail.
        let _infallible = match field.name() {
            "message" => self.message.write_fmt(value),
            name => write!(self.fields, " {name}={value}"),
        };
    }

    fn into_message(self) -> String {
        let mut text = self.message;
        text.push_str(&self.fields);
        cut_to_limit(&mut text, MAX_RECORD_MESSAGE_BYTES);
        text
    }
}

impl Visit for FieldText {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.append(field, format_args!("{value}"));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.append(field, format_args!("{value}"));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.append(field, format_args!("{value:?}"));
    }
}

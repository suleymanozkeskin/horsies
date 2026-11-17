//! A capture of the `tracing` events of one test.
//!
//! [`LogCapture::set_default`] makes the capture the default dispatcher of
//! the test thread. The events of the test thread reach it, and so do the
//! events of:
//! - a task of a current-thread runtime, which runs on the test thread;
//! - a task spawned with the current subscriber attached
//!   (`WithSubscriber::with_current_subscriber`), such as the runner beat
//!   writer;
//! - a thread that the worker starts with the dispatcher of its starter
//!   (`worker::thread_dispatch::with_starter_dispatch`), such as the runner
//!   beat watchdog and the spawner thread, when the test thread starts it.
//!
//! The capture keeps at most [`MAX_CAPTURED_EVENTS`] events and counts the
//! events past the bound.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// The most events one capture keeps.
pub(crate) const MAX_CAPTURED_EVENTS: usize = 10_000;

/// One captured event: its level, its target, its message, and every other
/// field as `name=value` text in the order of the event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedEvent {
    pub level: tracing::Level,
    pub target: String,
    pub message: String,
    pub fields: Vec<(String, String)>,
}

impl CapturedEvent {
    /// The text of the field `name`, if the event has it.
    pub(crate) fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.as_str())
    }
}

#[derive(Default)]
struct Captured {
    events: Mutex<Vec<CapturedEvent>>,
    past_bound: AtomicU64,
    last_span: AtomicU64,
}

/// A subscriber that keeps every event it receives, up to its bound.
#[derive(Clone, Default)]
pub(crate) struct LogCapture {
    captured: Arc<Captured>,
}

impl LogCapture {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Make the capture the default dispatcher of the calling thread until
    /// the guard drops.
    pub(crate) fn set_default(&self) -> tracing::subscriber::DefaultGuard {
        tracing::subscriber::set_default(self.clone())
    }

    /// Every kept event, in arrival order.
    pub(crate) fn events(&self) -> Vec<CapturedEvent> {
        self.captured
            .events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The kept events at `level` whose message starts with `start`.
    pub(crate) fn lines_starting(&self, level: tracing::Level, start: &str) -> Vec<CapturedEvent> {
        self.events()
            .into_iter()
            .filter(|event| event.level == level && event.message.starts_with(start))
            .collect()
    }

    /// The events that arrived after [`MAX_CAPTURED_EVENTS`] were kept.
    pub(crate) fn events_past_bound(&self) -> u64 {
        self.captured.past_bound.load(Ordering::Relaxed)
    }
}

/// The message and the other fields of one event.
#[derive(Default)]
struct EventText {
    message: String,
    fields: Vec<(String, String)>,
}

impl EventText {
    fn keep(&mut self, field: &tracing::field::Field, value: String) {
        match field.name() {
            "message" => self.message = value,
            name => self.fields.push((name.to_owned(), value)),
        }
    }
}

impl tracing::field::Visit for EventText {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.keep(field, value.to_owned());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.keep(field, format!("{value:?}"));
    }
}

impl tracing::Subscriber for LogCapture {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        // A span id is never 0.
        tracing::span::Id::from_u64(self.captured.last_span.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut text = EventText::default();
        event.record(&mut text);
        let mut events = self
            .captured
            .events
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match events.len() < MAX_CAPTURED_EVENTS {
            true => events.push(CapturedEvent {
                level: *event.metadata().level(),
                target: event.metadata().target().to_owned(),
                message: text.message,
                fields: text.fields,
            }),
            false => {
                let _counted = self.captured.past_bound.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_event_keeps_its_level_target_message_and_fields() {
        let capture = LogCapture::new();
        let _default = capture.set_default();

        tracing::warn!(attempt = 2, cause = %"refused", "a write failed");

        let [event]: [CapturedEvent; 1] = capture.events().try_into().expect("one event");
        assert_eq!(event.level, tracing::Level::WARN);
        assert_eq!(event.target, module_path!());
        assert_eq!(event.message, "a write failed");
        assert_eq!(event.field("attempt"), Some("2"));
        assert_eq!(event.field("cause"), Some("refused"));
        assert_eq!(
            capture
                .lines_starting(tracing::Level::WARN, "a write")
                .len(),
            1
        );
        assert!(capture
            .lines_starting(tracing::Level::ERROR, "a write")
            .is_empty());
    }

    #[test]
    fn events_past_the_bound_are_counted_and_not_kept() {
        let capture = LogCapture::new();
        let _default = capture.set_default();

        for _ in 0..MAX_CAPTURED_EVENTS + 3 {
            tracing::info!("one event");
        }

        assert_eq!(capture.events().len(), MAX_CAPTURED_EVENTS);
        assert_eq!(capture.events_past_bound(), 3);
    }
}

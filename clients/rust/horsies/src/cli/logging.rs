//! The logs of the runner: one `tracing` subscriber on stderr, at the
//! `--log-level` of the command, or at `RUST_LOG` when it is set, in the
//! [`LogFormat`] of the runner. The core's `tracing` events reach it. A
//! worker child installs the same subscriber at the level of its init
//! frame, the worker's `--log-level`, in the same format.
//!
//! With the feature `otel`, the subscriber also has the layer of
//! `horsies::otel`, and its filter passes the attempt span of each task
//! body at every level.
//!
//! A subscriber that the binary installed before `run` is kept.

use std::io::IsTerminal;

use horsies_core::frame_protocol::ChildLogLevel;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use super::LogFormat;
use crate::WorkerLogLevel;

/// Install the subscriber at `level` in `format` (Python `setup_logging`).
/// A global subscriber that exists already is kept.
pub(crate) fn install_logs(level: WorkerLogLevel, format: LogFormat) {
    install(filter_text(level), format);
}

/// The log setup of a worker child: the subscriber at the level of its init
/// frame, in `format`.
pub(super) fn install_child_logs(level: ChildLogLevel, format: LogFormat) {
    install(filter_text(worker_level(level)), format);
}

fn install(level: &'static str, format: LogFormat) {
    let color = resolve_color(
        std::env::var("FORCE_COLOR").ok().as_deref(),
        std::env::var_os("NO_COLOR").is_some(),
        std::io::stderr().is_terminal(),
    );
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let subscriber = tracing_subscriber::registry()
        .with(runner_filter(filter))
        .with(format_layer(format, color, std::io::stderr));
    #[cfg(feature = "otel")]
    let subscriber = subscriber.with(crate::otel::layer());
    // A subscriber that the binary installed first stays: the runner adds
    // none.
    let _kept_or_installed = subscriber.try_init();
}

/// The filter of the runner from `filter` (`RUST_LOG` when it is set and
/// reads, else the level of the command), with the span of each task body
/// passed at every level: every body has its span, and its records carry
/// the task's fields, with or without the feature `otel`. Pure.
fn runner_filter(filter: EnvFilter) -> EnvFilter {
    filter.add_directive(crate::telemetry::attempt_span_directive())
}

/// The layer that writes each record to `writer` in `format`:
/// - [`LogFormat::TextLogs`]: one line of text, with ANSI color when `color`;
/// - [`LogFormat::JsonLogs`]: one JSON object per line, no color: `timestamp`,
///   `level`, `target`, the fields of the event at the top level
///   (`message` among them), `span` (the innermost span, with its name and
///   fields) and `spans` (every span of the record, outermost first).
fn format_layer<S, W>(
    format: LogFormat,
    color: bool,
    writer: W,
) -> Box<dyn Layer<S> + Send + Sync + 'static>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    match format {
        LogFormat::TextLogs => tracing_subscriber::fmt::layer()
            .with_writer(writer)
            .with_ansi(color)
            .with_target(true)
            .boxed(),
        LogFormat::JsonLogs => tracing_subscriber::fmt::layer()
            .json()
            .with_writer(writer)
            .with_ansi(false)
            .with_target(true)
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(true)
            .boxed(),
    }
}

/// The `tracing` filter of a level. `tracing` has no level above `error`,
/// so Python's `CRITICAL` logs errors. Pure.
fn filter_text(level: WorkerLogLevel) -> &'static str {
    match level {
        WorkerLogLevel::Debug => "debug",
        WorkerLogLevel::Info => "info",
        WorkerLogLevel::Warning => "warn",
        WorkerLogLevel::Error | WorkerLogLevel::Critical => "error",
    }
}

/// The worker level of a child's init frame level. Pure.
fn worker_level(level: ChildLogLevel) -> WorkerLogLevel {
    match level {
        ChildLogLevel::Debug => WorkerLogLevel::Debug,
        ChildLogLevel::Info => WorkerLogLevel::Info,
        ChildLogLevel::Warning => WorkerLogLevel::Warning,
        ChildLogLevel::Error => WorkerLogLevel::Error,
        ChildLogLevel::Critical => WorkerLogLevel::Critical,
    }
}

/// Whether the logs carry ANSI color: `FORCE_COLOR` (set, not empty, not
/// `0`) turns it on and wins over `NO_COLOR`; else `NO_COLOR` (set, any
/// value) turns it off; else color only on a terminal. Pure.
fn resolve_color(force_color: Option<&str>, no_color: bool, is_terminal: bool) -> bool {
    match (force_color, no_color) {
        (Some(force), _) if !force.is_empty() && force != "0" => true,
        (_, true) => false,
        (_, false) => is_terminal,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, PoisonError};

    use super::*;

    /// A writer whose bytes go to one shared buffer.
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// The lines that the JSON layer writes for one event in a span with
    /// the task's fields, inside a second span.
    fn json_lines() -> Vec<String> {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer_buffer = Arc::clone(&buffer);
        let layer = format_layer(LogFormat::JsonLogs, true, move || {
            Captured(Arc::clone(&writer_buffer))
        });
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            let run = tracing::info_span!(
                "run",
                horsies.task.id = "0193f0aa-7b1c-7a3e-9a51-3c1d2e4f5a6b",
                horsies.task.name = "app.tasks.charge",
            );
            run.in_scope(|| {
                tracing::info_span!("charge_card", cents = 1_200_u32).in_scope(|| {
                    tracing::warn!(attempt = 2_u32, "the card was declined");
                });
            });
        });
        let text = String::from_utf8(
            buffer
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        )
        .expect("the JSON layer writes UTF-8");
        text.lines().map(str::to_owned).collect()
    }

    /// `LogFormat::JsonLogs` writes one JSON object per record, with the level,
    /// the target, the fields of the event at the top level, the innermost
    /// span and every span of the record with their fields, and no color.
    #[test]
    fn the_json_format_writes_one_object_per_record_with_its_spans() {
        let lines = json_lines();
        let [line] = lines.as_slice() else {
            panic!("one line per record, not {lines:?}");
        };
        assert!(!line.contains('\u{1b}'), "no ANSI color: {line}");
        let record: serde_json::Value = serde_json::from_str(line).expect("one JSON object");
        assert!(record["timestamp"].is_string(), "{record}");
        assert_eq!(record["level"], "WARN");
        assert_eq!(record["target"], module_path!());
        assert_eq!(record["message"], "the card was declined");
        assert_eq!(record["attempt"], 2);
        assert_eq!(record["span"]["name"], "charge_card");
        assert_eq!(record["span"]["cents"], 1_200);
        let spans = record["spans"].as_array().expect("the span list");
        assert_eq!(spans.len(), 2, "{record}");
        assert_eq!(spans[0]["name"], "run");
        assert_eq!(
            spans[0]["horsies.task.id"],
            "0193f0aa-7b1c-7a3e-9a51-3c1d2e4f5a6b"
        );
        assert_eq!(spans[0]["horsies.task.name"], "app.tasks.charge");
        assert_eq!(spans[1]["name"], "charge_card");
    }

    /// The filter of the runner keeps its level and passes the span of each
    /// task body.
    #[test]
    fn the_filter_of_the_runner_keeps_its_level() {
        let filter = runner_filter(EnvFilter::new("warn")).to_string();
        assert!(filter.contains("warn"), "{filter}");
        assert!(
            filter.contains(&crate::telemetry::attempt_span_directive().to_string()),
            "{filter}"
        );
    }

    #[test]
    fn force_color_wins_over_no_color_and_the_terminal() {
        assert!(resolve_color(Some("1"), true, false));
        assert!(resolve_color(Some("yes"), false, false));
    }

    #[test]
    fn an_empty_or_zero_force_color_falls_through() {
        assert!(!resolve_color(Some("0"), false, false));
        assert!(!resolve_color(Some(""), true, true));
        assert!(resolve_color(Some("0"), false, true));
    }

    #[test]
    fn no_color_turns_color_off_on_a_terminal() {
        assert!(!resolve_color(None, true, true));
        assert!(resolve_color(None, false, true));
        assert!(!resolve_color(None, false, false));
    }

    #[test]
    fn each_level_has_its_filter_and_critical_logs_errors() {
        assert_eq!(filter_text(WorkerLogLevel::Warning), "warn");
        assert_eq!(filter_text(WorkerLogLevel::Critical), "error");
        assert_eq!(
            worker_level(ChildLogLevel::Critical),
            WorkerLogLevel::Critical
        );
        assert_eq!(worker_level(ChildLogLevel::Debug), WorkerLogLevel::Debug);
    }
}

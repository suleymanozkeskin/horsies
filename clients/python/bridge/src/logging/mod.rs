//! The log bridge: core `tracing` events to Python `logging`.
//!
//! - The subscriber checks each event against the level snapshot, reads its
//!   fields, and pushes one record into a bounded channel. A core thread
//!   never waits for the GIL to log.
//! - The record's message is the event's `message` field alone. Every other
//!   field goes on the Python `LogRecord` as an attribute, as the keys of
//!   `extra` do; the forwarder states the rule for a field whose name the
//!   `LogRecord` already uses.
//! - One forwarder thread takes the GIL and hands the records to Python
//!   `logging` with the core's time and thread name. Records arrive after the
//!   event, from the forwarder thread.
//! - When the channel is full, a DEBUG or INFO record is dropped and counted.
//!   A WARNING or ERROR record is never dropped: it waits up to
//!   [`HIGH_RECORD_WAIT`], then goes to stderr with the drop counts. The
//!   forwarder logs one warning with the counts when space returns.
//! - Python `logging` is the only configuration surface: the forwarder reads
//!   `getEffectiveLevel()` of each logger at start, every
//!   [`forwarder::LEVEL_REFRESH_INTERVAL`], and on `refresh_log_levels()`.
//! - Before a worker process aborts, it waits up to a bound until the
//!   forwarder has handled every record in the channel
//!   ([`LogBridge::wait_for_drain`]).
//!
//! The subscriber is the default dispatcher of the runtime threads and of
//! every bridged operation; it is never the process-global default, so it
//! does not take the place of another library's subscriber.

mod forwarder;
mod levels;
mod names;
mod subscriber;

use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use pyo3::prelude::*;

pub use forwarder::start_forwarder;
pub use levels::RecordLevel;
pub use names::LoggerName;

use levels::LevelSnapshot;

/// Records the channel holds. A push into a full channel follows the drop
/// rule of the module.
pub const LOG_CHANNEL_CAPACITY: usize = 8_192;

/// How long a WARNING or ERROR record waits for space in a full channel
/// before it goes to stderr.
pub const HIGH_RECORD_WAIT: Duration = Duration::from_millis(100);

/// The pause between two push attempts of a waiting record.
const HIGH_RECORD_RETRY_PAUSE: Duration = Duration::from_millis(1);

/// Push attempts of a waiting record: [`HIGH_RECORD_WAIT`] in steps of
/// [`HIGH_RECORD_RETRY_PAUSE`].
const HIGH_RECORD_ATTEMPTS: u128 =
    HIGH_RECORD_WAIT.as_millis() / HIGH_RECORD_RETRY_PAUSE.as_millis();

/// The pause between two reads of the channel count while a drain wait runs.
const DRAIN_POLL_PAUSE: Duration = Duration::from_millis(5);

/// Largest text of one record, in bytes: its message and its text field
/// values together. A text that reaches the bound is cut at a character
/// boundary and marked.
pub const MAX_RECORD_TEXT_BYTES: usize = 16 * 1024;

/// Most fields of one record besides its message.
pub const MAX_RECORD_FIELDS: usize = 32;

/// The mark at the end of a cut message.
pub const CUT_MARK: &str = " [message cut]";

/// The mark at the end of a cut text field value.
pub const VALUE_CUT_MARK: &str = " [value cut]";

/// Where in the source an event was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordSource {
    Known { file: &'static str, line: u32 },
    Unknown,
}

/// The value of one field of a core event, as the event records it.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    Bool(bool),
    /// A signed integer (`i64` or `i128` in the event).
    Signed(i128),
    /// An unsigned integer (`u64` or `u128` in the event).
    Unsigned(u128),
    Float(f64),
    /// Any other value: its `Display` text (a `&str`, an error, a `%value`)
    /// or its `Debug` text (a `?value`).
    Text(String),
}

impl fmt::Display for FieldValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool(value) => write!(formatter, "{value}"),
            Self::Signed(value) => write!(formatter, "{value}"),
            Self::Unsigned(value) => write!(formatter, "{value}"),
            Self::Float(value) => write!(formatter, "{value}"),
            Self::Text(value) => formatter.write_str(value),
        }
    }
}

/// One field of a core event other than `message`.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordField {
    /// The name that the event's callsite gives the field.
    pub name: &'static str,
    pub value: FieldValue,
}

/// One core log record on its way to Python.
#[derive(Clone, Debug, PartialEq)]
pub struct CoreLogRecord {
    pub logger: LoggerName,
    pub level: RecordLevel,
    /// The event's `message` field.
    pub message: String,
    /// The other fields of the event, in the order that the event records
    /// them.
    pub fields: Vec<RecordField>,
    pub source: RecordSource,
    pub created: SystemTime,
    pub thread_name: String,
}

impl CoreLogRecord {
    /// A record of the bridge itself, created now on this thread. It has no
    /// fields.
    pub fn bridge(level: RecordLevel, message: String) -> Self {
        Self {
            logger: LoggerName::Bridge,
            level,
            message,
            fields: Vec::new(),
            source: RecordSource::Unknown,
            created: SystemTime::now(),
            thread_name: current_thread_name(),
        }
    }

    /// The fields as ` name=value` text, for a line on stderr: a record that
    /// goes to stderr has no `LogRecord` to hold them.
    fn fields_text(&self) -> FieldsText<'_> {
        FieldsText(&self.fields)
    }
}

/// The fields of a record as ` name=value` text, one after the other.
struct FieldsText<'a>(&'a [RecordField]);

impl fmt::Display for FieldsText<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for field in self.0 {
            write!(formatter, " {}={}", field.name, field.value)?;
        }
        Ok(())
    }
}

/// The name of the current thread, or its id when it has no name.
pub fn current_thread_name() -> String {
    let thread = std::thread::current();
    match thread.name() {
        Some(name) => name.to_owned(),
        None => format!("{:?}", thread.id()),
    }
}

/// Records lost to a full channel since the last report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DropCounts {
    /// DEBUG and INFO records dropped.
    pub dropped_low: u64,
    /// WARNING and ERROR records written to stderr instead.
    pub spilled_high: u64,
}

/// How a wait for the forwarder to empty the channel ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The forwarder handled every record that was in the channel: each one
    /// reached Python `logging` or stderr.
    Drained,
    /// The forwarder thread is not running, so `left` records stay in the
    /// channel.
    ForwarderStopped { left: u64 },
    /// The bound passed with `left` records still in the channel.
    BoundReached { left: u64 },
}

/// The sending side of the channel, the level snapshot and the counters.
pub struct LogSink {
    sender: SyncSender<CoreLogRecord>,
    levels: LevelSnapshot,
    dropped_low: AtomicU64,
    spilled_high: AtomicU64,
    /// Records in the channel that the forwarder has not handled yet. A push
    /// counts a record before it sends it and uncounts it when the send
    /// fails, so the forwarder never uncounts a record that was not counted.
    in_channel: AtomicU64,
    /// The forwarder thread runs its service loop.
    forwarder_running: AtomicBool,
}

impl LogSink {
    /// Whether Python would handle a record of `level` on `logger`.
    pub fn enabled(&self, logger: LoggerName, level: RecordLevel) -> bool {
        self.levels.enabled(logger, level)
    }

    /// Push one record. Never waits for the GIL; a WARNING or ERROR record
    /// in a full channel waits up to [`HIGH_RECORD_WAIT`].
    pub fn push(&self, record: CoreLogRecord) {
        match self.try_enqueue(record) {
            Ok(()) => {}
            Err(TrySendError::Full(record)) => match record.level.waits_when_full() {
                false => {
                    let _previous = self.dropped_low.fetch_add(1, Ordering::Relaxed);
                }
                true => self.push_waiting(record),
            },
            Err(TrySendError::Disconnected(record)) => self.spill(&record),
        }
    }

    fn push_waiting(&self, record: CoreLogRecord) {
        let mut waiting = record;
        for _attempt in 0..HIGH_RECORD_ATTEMPTS {
            std::thread::sleep(HIGH_RECORD_RETRY_PAUSE);
            match self.try_enqueue(waiting) {
                Ok(()) => return,
                Err(TrySendError::Full(record)) => waiting = record,
                Err(TrySendError::Disconnected(record)) => {
                    self.spill(&record);
                    return;
                }
            }
        }
        let _previous = self.spilled_high.fetch_add(1, Ordering::Relaxed);
        self.spill(&waiting);
    }

    /// Send one record into the channel, counted while it is there.
    fn try_enqueue(&self, record: CoreLogRecord) -> Result<(), TrySendError<CoreLogRecord>> {
        let _previous = self.in_channel.fetch_add(1, Ordering::SeqCst);
        let sent = self.sender.try_send(record);
        if sent.is_err() {
            let _previous = self.in_channel.fetch_sub(1, Ordering::SeqCst);
        }
        sent
    }

    /// The forwarder handled `count` records: they reached Python `logging`
    /// or stderr.
    pub fn records_handled(&self, count: usize) {
        let _previous = self.in_channel.fetch_sub(count as u64, Ordering::SeqCst);
    }

    /// The forwarder thread entered its service loop.
    pub fn mark_forwarder_running(&self) {
        self.forwarder_running.store(true, Ordering::SeqCst);
    }

    /// The forwarder thread left its service loop, or never started.
    pub fn mark_forwarder_stopped(&self) {
        self.forwarder_running.store(false, Ordering::SeqCst);
    }

    /// Wait until the forwarder has handled every record in the channel, the
    /// forwarder stops, or `bound` passes. Reads only counters; the caller
    /// must not be attached to Python, or the forwarder cannot take the GIL.
    pub fn wait_for_drain(&self, bound: Duration) -> DrainOutcome {
        let polls = bound.as_millis() / DRAIN_POLL_PAUSE.as_millis();
        for _poll in 0..=polls {
            let left = self.in_channel.load(Ordering::SeqCst);
            match (left, self.forwarder_running.load(Ordering::SeqCst)) {
                (0, _) => return DrainOutcome::Drained,
                (_, false) => return DrainOutcome::ForwarderStopped { left },
                (_, true) => std::thread::sleep(DRAIN_POLL_PAUSE),
            }
        }
        DrainOutcome::BoundReached {
            left: self.in_channel.load(Ordering::SeqCst),
        }
    }

    /// Write a record that cannot reach Python to stderr, with the counts.
    fn spill(&self, record: &CoreLogRecord) {
        let _write_failure_ignored = writeln!(
            std::io::stderr(),
            "{} {:?} {}{} (horsies log channel full or closed; dropped DEBUG/INFO records: {}, \
             WARNING/ERROR records written to stderr: {})",
            record.logger.python_name(),
            record.level,
            record.message,
            record.fields_text(),
            self.dropped_low.load(Ordering::Relaxed),
            self.spilled_high.load(Ordering::Relaxed),
        );
    }

    /// Take the counts since the last report, and reset them.
    pub fn take_drop_counts(&self) -> DropCounts {
        DropCounts {
            dropped_low: self.dropped_low.swap(0, Ordering::Relaxed),
            spilled_high: self.spilled_high.swap(0, Ordering::Relaxed),
        }
    }

    /// Copy Python's effective level of every logger into the snapshot.
    /// The caller is attached.
    pub fn refresh_levels(&self, py: Python<'_>) -> PyResult<()> {
        let logging = py.import("logging")?;
        for logger in LoggerName::ALL {
            let python_logger = logging.call_method1("getLogger", (logger.python_name(),))?;
            let level: i64 = python_logger.call_method0("getEffectiveLevel")?.extract()?;
            self.levels.store(logger, level);
        }
        Ok(())
    }
}

/// The log bridge of one process: the sink and the dispatcher built on it.
pub struct LogBridge {
    sink: Arc<LogSink>,
    dispatch: tracing::Dispatch,
}

impl LogBridge {
    /// A bridge and the receiving side of its channel. Starts no thread.
    pub fn new() -> (Self, Receiver<CoreLogRecord>) {
        let (sender, receiver) = std::sync::mpsc::sync_channel(LOG_CHANNEL_CAPACITY);
        let sink = Arc::new(LogSink {
            sender,
            levels: LevelSnapshot::new(),
            dropped_low: AtomicU64::new(0),
            spilled_high: AtomicU64::new(0),
            in_channel: AtomicU64::new(0),
            forwarder_running: AtomicBool::new(false),
        });
        let dispatch = tracing::Dispatch::new(subscriber::CoreSubscriber::new(Arc::clone(&sink)));
        (Self { sink, dispatch }, receiver)
    }

    /// The dispatcher of the core subscriber.
    pub fn dispatch(&self) -> &tracing::Dispatch {
        &self.dispatch
    }

    /// The sink, shared with the forwarder thread.
    pub fn sink(&self) -> Arc<LogSink> {
        Arc::clone(&self.sink)
    }

    /// Push a record of the bridge itself, if Python would handle it.
    pub fn push_bridge_record(&self, level: RecordLevel, message: String) {
        if self.sink.enabled(LoggerName::Bridge, level) {
            self.sink.push(CoreLogRecord::bridge(level, message));
        }
    }

    /// Copy Python's effective levels into the snapshot. The caller is
    /// attached.
    pub fn refresh_levels(&self, py: Python<'_>) -> PyResult<()> {
        self.sink.refresh_levels(py)
    }

    /// Wait up to `bound` until the forwarder has handled every record in
    /// the channel. The caller is not attached to Python.
    pub fn wait_for_drain(&self, bound: Duration) -> DrainOutcome {
        self.sink.wait_for_drain(bound)
    }
}

/// Make `dispatch` the default dispatcher of the current thread for the
/// life of the thread. Runtime threads call it when they start.
pub fn make_thread_default(dispatch: &tracing::Dispatch) {
    // The guard would restore the earlier default when dropped; the runtime
    // thread keeps this default until it ends, so the guard is not dropped.
    std::mem::forget(tracing::dispatcher::set_default(dispatch));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(level: RecordLevel) -> CoreLogRecord {
        CoreLogRecord::bridge(level, "message".to_owned())
    }

    fn sink_with_capacity(capacity: usize) -> (LogSink, Receiver<CoreLogRecord>) {
        let (sender, receiver) = std::sync::mpsc::sync_channel(capacity);
        let sink = LogSink {
            sender,
            levels: LevelSnapshot::new(),
            dropped_low: AtomicU64::new(0),
            spilled_high: AtomicU64::new(0),
            in_channel: AtomicU64::new(0),
            forwarder_running: AtomicBool::new(false),
        };
        (sink, receiver)
    }

    #[test]
    fn only_a_record_that_entered_the_channel_is_counted() {
        let (sink, _receiver) = sink_with_capacity(1);
        sink.push(record(RecordLevel::Info));
        sink.push(record(RecordLevel::Debug));
        assert_eq!(sink.in_channel.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_drain_wait_ends_when_the_forwarder_handled_every_record() {
        let (sink, receiver) = sink_with_capacity(4);
        let sink = Arc::new(sink);
        sink.mark_forwarder_running();
        sink.push(record(RecordLevel::Info));
        sink.push(record(RecordLevel::Error));
        let forwarder_sink = Arc::clone(&sink);
        let forwarder = std::thread::spawn(move || {
            let taken = receiver.iter().take(2).count();
            forwarder_sink.records_handled(taken);
        });
        assert_eq!(
            sink.wait_for_drain(Duration::from_secs(5)),
            DrainOutcome::Drained
        );
        forwarder.join().expect("the forwarder stand-in ends");
    }

    #[test]
    fn a_drain_wait_with_a_stopped_forwarder_names_the_records_left() {
        let (sink, _receiver) = sink_with_capacity(4);
        sink.push(record(RecordLevel::Error));
        sink.mark_forwarder_stopped();
        assert_eq!(
            sink.wait_for_drain(Duration::from_secs(5)),
            DrainOutcome::ForwarderStopped { left: 1 }
        );
    }

    #[test]
    fn a_drain_wait_ends_at_its_bound() {
        let (sink, _receiver) = sink_with_capacity(4);
        sink.mark_forwarder_running();
        sink.push(record(RecordLevel::Error));
        assert_eq!(
            sink.wait_for_drain(Duration::from_millis(20)),
            DrainOutcome::BoundReached { left: 1 }
        );
    }

    #[test]
    fn a_record_with_space_reaches_the_channel() {
        let (sink, receiver) = sink_with_capacity(1);
        sink.push(record(RecordLevel::Info));
        assert_eq!(
            receiver.try_recv().map(|kept| kept.level),
            Ok(RecordLevel::Info)
        );
        assert_eq!(
            sink.take_drop_counts(),
            DropCounts {
                dropped_low: 0,
                spilled_high: 0
            }
        );
    }

    #[test]
    fn a_low_record_in_a_full_channel_is_dropped_and_counted() {
        let (sink, _receiver) = sink_with_capacity(1);
        sink.push(record(RecordLevel::Info));
        sink.push(record(RecordLevel::Debug));
        assert_eq!(
            sink.take_drop_counts(),
            DropCounts {
                dropped_low: 1,
                spilled_high: 0
            }
        );
        assert_eq!(
            sink.take_drop_counts(),
            DropCounts {
                dropped_low: 0,
                spilled_high: 0
            }
        );
    }

    #[test]
    fn a_high_record_in_a_full_channel_waits_then_goes_to_stderr() {
        let (sink, receiver) = sink_with_capacity(1);
        sink.push(record(RecordLevel::Info));
        let started = std::time::Instant::now();
        sink.push(record(RecordLevel::Warning));
        assert!(started.elapsed() >= HIGH_RECORD_WAIT);
        assert_eq!(
            sink.take_drop_counts(),
            DropCounts {
                dropped_low: 0,
                spilled_high: 1
            }
        );
        assert_eq!(
            receiver.try_recv().map(|kept| kept.level),
            Ok(RecordLevel::Info)
        );
    }

    #[test]
    fn a_high_record_takes_space_that_returns_while_it_waits() {
        let (sink, receiver) = sink_with_capacity(1);
        sink.push(record(RecordLevel::Info));
        let reader = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            let first = receiver.recv().map(|kept| kept.level);
            let second = receiver.recv().map(|kept| kept.level);
            (first, second)
        });
        sink.push(record(RecordLevel::Error));
        let (first, second) = reader.join().expect("the reader thread ends");
        assert_eq!(first, Ok(RecordLevel::Info));
        assert_eq!(second, Ok(RecordLevel::Error));
        assert_eq!(
            sink.take_drop_counts(),
            DropCounts {
                dropped_low: 0,
                spilled_high: 0
            }
        );
    }

    #[test]
    fn the_stderr_text_of_the_fields_is_each_name_and_value_after_a_space() {
        let mut logged = record(RecordLevel::Error);
        assert_eq!(logged.fields_text().to_string(), "");
        logged.fields = vec![
            RecordField {
                name: "read",
                value: FieldValue::Text("workflow names".to_owned()),
            },
            RecordField {
                name: "bound",
                value: FieldValue::Unsigned(10_000),
            },
            RecordField {
                name: "retry",
                value: FieldValue::Bool(false),
            },
        ];
        assert_eq!(
            logged.fields_text().to_string(),
            " read=workflow names bound=10000 retry=false"
        );
    }
}

//! The log bridge of the process: the records, the bounded channel, the
//! drop rule, the drain wait, and the start of the bridge.
//!
//! - The subscriber pushes one record per enabled event into a channel of
//!   [`LOG_CHANNEL_CAPACITY`] records. A core thread never waits for the
//!   JavaScript thread to log.
//! - When the channel is full, a TRACE, DEBUG or INFO record is dropped and
//!   counted. A WARNING or ERROR record is never dropped: it waits up to
//!   [`HIGH_RECORD_WAIT`], then goes to stderr with the drop counts. The
//!   forwarder logs one warning with the counts when space returns.
//! - One forwarder thread hands the records to the sink: one stderr line per
//!   record, or the JavaScript sink that the package set.
//! - Before a worker process aborts, it waits up to a bound until the
//!   forwarder has handled every record in the channel
//!   ([`LogBridge::wait_for_drain`]).
//! - The bridge starts once per process, at the first core call or at the log
//!   setup of a child. Its subscriber is the global default dispatcher of
//!   this addon: the `tracing` statics of this library serve only this
//!   addon, so it takes no other subscriber's place, and the events of every
//!   core thread reach the sink.

use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, SystemTime};

use chrono::{DateTime, SecondsFormat, Utc};
use horsies_core::frame_protocol::ChildLogLevel;
use horsies_core::ChildLogSetup;

use super::format::{current_format, line_in, notice_line, LogFormatKind};
use super::forwarder;
use super::levels::{LogThreshold, RecordLevel, WhenFull, LEVELS};
use super::names::LoggerName;
use super::subscriber::CoreSubscriber;

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

/// One core log record on its way to the sink.
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

    /// The record as one stderr line: the time in UTC, the level, the
    /// logger, the message, and each field as ` name=value`.
    pub fn stderr_line(&self) -> String {
        format!(
            "{} {} {} {}{}",
            DateTime::<Utc>::from(self.created).to_rfc3339_opts(SecondsFormat::Micros, true),
            self.level.text(),
            self.logger.dotted_name(),
            self.message,
            FieldsText(&self.fields),
        )
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

/// Records lost since the last report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DropCounts {
    /// TRACE, DEBUG and INFO records dropped in a full channel.
    pub dropped_low: u64,
    /// WARNING and ERROR records written to stderr in place of the channel.
    pub spilled_high: u64,
    /// Records of batches that the JavaScript sink did not take within the
    /// answer bound.
    pub abandoned: u64,
}

/// How a wait for the forwarder to empty the channel ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The forwarder handled every record that was in the channel: each one
    /// reached the sink or stderr.
    Drained,
    /// The forwarder thread is not running, so `left` records stay in the
    /// channel.
    ForwarderStopped { left: u64 },
    /// The bound passed with `left` records still in the channel.
    BoundReached { left: u64 },
}

/// The sending side of the channel and its counters.
pub struct LogSink {
    sender: SyncSender<CoreLogRecord>,
    dropped_low: AtomicU64,
    spilled_high: AtomicU64,
    abandoned: AtomicU64,
    /// Records in the channel that the forwarder has not handled yet. A push
    /// counts a record before it sends it and uncounts it when the send
    /// fails, so the forwarder never uncounts a record that was not counted.
    in_channel: AtomicU64,
    /// The forwarder thread runs its service loop.
    forwarder_running: AtomicBool,
    /// The forwarder writes every later record to stderr, not to the
    /// JavaScript sink: the environment ends, or the process aborts from the
    /// JavaScript thread. It never turns back.
    stderr_only: AtomicBool,
}

impl LogSink {
    /// A sink over a channel of `capacity` records, and the receiving side.
    /// Starts no thread.
    pub fn new(capacity: usize) -> (Self, Receiver<CoreLogRecord>) {
        let (sender, receiver) = std::sync::mpsc::sync_channel(capacity);
        let sink = Self {
            sender,
            dropped_low: AtomicU64::new(0),
            spilled_high: AtomicU64::new(0),
            abandoned: AtomicU64::new(0),
            in_channel: AtomicU64::new(0),
            forwarder_running: AtomicBool::new(false),
            stderr_only: AtomicBool::new(false),
        };
        (sink, receiver)
    }

    /// Push one record. Never waits for the JavaScript thread; a WARNING or
    /// ERROR record in a full channel waits up to [`HIGH_RECORD_WAIT`].
    pub fn push(&self, record: CoreLogRecord) {
        match self.try_enqueue(record) {
            Ok(()) => {}
            Err(refused) => match *refused {
                TrySendError::Full(record) => match record.level.when_full() {
                    WhenFull::Drop => {
                        let _previous = self.dropped_low.fetch_add(1, Ordering::Relaxed);
                    }
                    WhenFull::WaitThenSpill => self.push_waiting(record),
                },
                TrySendError::Disconnected(record) => self.spill(&record),
            },
        }
    }

    fn push_waiting(&self, record: CoreLogRecord) {
        let mut waiting = record;
        for _attempt in 0..HIGH_RECORD_ATTEMPTS {
            std::thread::sleep(HIGH_RECORD_RETRY_PAUSE);
            match self.try_enqueue(waiting) {
                Ok(()) => return,
                Err(refused) => match *refused {
                    TrySendError::Full(record) => waiting = record,
                    TrySendError::Disconnected(record) => {
                        self.spill(&record);
                        return;
                    }
                },
            }
        }
        let _previous = self.spilled_high.fetch_add(1, Ordering::Relaxed);
        self.spill(&waiting);
    }

    /// Send one record into the channel, counted while it is there. A
    /// refused send gives the record back in a box: the record is large, and
    /// only a full or closed channel pays for the box.
    fn try_enqueue(&self, record: CoreLogRecord) -> Result<(), Box<TrySendError<CoreLogRecord>>> {
        let _previous = self.in_channel.fetch_add(1, Ordering::SeqCst);
        let sent = self.sender.try_send(record);
        if sent.is_err() {
            let _previous = self.in_channel.fetch_sub(1, Ordering::SeqCst);
        }
        sent.map_err(Box::new)
    }

    /// The forwarder handled `count` records: they reached the sink or
    /// stderr.
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

    /// From now on the forwarder writes every record to stderr.
    pub fn switch_to_stderr(&self) {
        self.stderr_only.store(true, Ordering::SeqCst);
    }

    /// Whether the forwarder writes every record to stderr.
    pub fn stderr_only(&self) -> bool {
        self.stderr_only.load(Ordering::SeqCst)
    }

    /// The state of the channel now, with no wait: `Drained` when no record
    /// waits, `ForwarderStopped` when records wait and the forwarder does not
    /// run, `BoundReached` when records wait for the running forwarder.
    /// Reads only counters.
    pub fn drain_state(&self) -> DrainOutcome {
        let left = self.in_channel.load(Ordering::SeqCst);
        match (left, self.forwarder_running.load(Ordering::SeqCst)) {
            (0, _) => DrainOutcome::Drained,
            (_, false) => DrainOutcome::ForwarderStopped { left },
            (_, true) => DrainOutcome::BoundReached { left },
        }
    }

    /// Wait until the forwarder has handled every record in the channel, the
    /// forwarder stops, or `bound` passes. Reads only counters.
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

    /// Write a record that cannot enter the channel to stderr, with the
    /// counts: in the text format one line with the counts after the record,
    /// in the JSON format the line of the record and then a notice line of
    /// the counts.
    fn spill(&self, record: &CoreLogRecord) {
        let counts = format!(
            "horsies log channel full or closed; dropped TRACE/DEBUG/INFO records: {}, \
             WARNING/ERROR records written to stderr: {}",
            self.dropped_low.load(Ordering::Relaxed),
            self.spilled_high.load(Ordering::Relaxed),
        );
        let text = match current_format() {
            LogFormatKind::TextLogs => format!("{} ({counts})", record.stderr_line()),
            LogFormatKind::JsonLogs => format!(
                "{}\n{}",
                line_in(LogFormatKind::JsonLogs, record),
                notice_line(counts)
            ),
        };
        let _write_failure_ignored = writeln!(std::io::stderr(), "{text}");
    }

    /// Count `count` records of a batch that the JavaScript sink did not
    /// take within the answer bound.
    pub fn records_abandoned(&self, count: usize) {
        let _previous = self.abandoned.fetch_add(count as u64, Ordering::Relaxed);
    }

    /// Take the counts since the last report, and reset them.
    pub fn take_drop_counts(&self) -> DropCounts {
        DropCounts {
            dropped_low: self.dropped_low.swap(0, Ordering::Relaxed),
            spilled_high: self.spilled_high.swap(0, Ordering::Relaxed),
            abandoned: self.abandoned.swap(0, Ordering::Relaxed),
        }
    }
}

/// The log bridge: the sink and the dispatcher built on it.
pub struct LogBridge {
    sink: Arc<LogSink>,
    dispatch: tracing::Dispatch,
}

impl LogBridge {
    /// A bridge and the receiving side of its channel. Starts no thread.
    pub fn new() -> (Self, Receiver<CoreLogRecord>) {
        let (sink, receiver) = LogSink::new(LOG_CHANNEL_CAPACITY);
        let sink = Arc::new(sink);
        let dispatch = tracing::Dispatch::new(CoreSubscriber::new(Arc::clone(&sink)));
        (Self { sink, dispatch }, receiver)
    }

    /// The dispatcher of the core subscriber.
    pub fn dispatch(&self) -> &tracing::Dispatch {
        &self.dispatch
    }

    /// The sink, shared with the forwarder thread.
    pub fn sink(&self) -> &Arc<LogSink> {
        &self.sink
    }

    /// Push a record of the bridge itself, if the threshold of the bridge
    /// logger lets it through.
    pub fn push_bridge_record(&self, level: RecordLevel, message: String) {
        self.push_record(CoreLogRecord::bridge(level, message));
    }

    /// Push `record`, if the threshold of its logger lets it through.
    pub fn push_record(&self, record: CoreLogRecord) {
        if LEVELS.enabled(record.logger, record.level) {
            self.sink.push(record);
        }
    }

    /// Wait up to `bound` until the forwarder has handled every record in
    /// the channel.
    pub fn wait_for_drain(&self, bound: Duration) -> DrainOutcome {
        self.sink.wait_for_drain(bound)
    }
}

/// The log forwarder thread could not start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogStartFailure {
    pub cause: String,
}

/// The started log bridge of the process.
static LOG: OnceLock<LogBridge> = OnceLock::new();

/// Held by a start, so two starts cannot both start a forwarder.
static LOG_START_LOCK: Mutex<()> = Mutex::new(());

/// The log bridge of the process. The first call starts it: the channel, the
/// forwarder thread, and the subscriber as the global default dispatcher of
/// this addon. A failed start leaves nothing started; a later call tries
/// again.
pub fn started_log() -> Result<&'static LogBridge, LogStartFailure> {
    if let Some(log) = LOG.get() {
        return Ok(log);
    }
    // The lock guards no data that a panic can break.
    let _start = LOG_START_LOCK
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if let Some(log) = LOG.get() {
        return Ok(log);
    }
    let (bridge, records) = LogBridge::new();
    forwarder::start_forwarder(records, bridge.sink()).map_err(|error| LogStartFailure {
        cause: error.to_string(),
    })?;
    // A default dispatcher set earlier in this addon stays; the runtime
    // threads and every guarded operation still use this bridge.
    let _earlier_default_kept = tracing::dispatcher::set_global_default(bridge.dispatch().clone());
    Ok(LOG.get_or_init(|| bridge))
}

/// The log bridge of the process when it started. Starts nothing.
pub fn running_log() -> Option<&'static LogBridge> {
    LOG.get()
}

/// Make `dispatch` the default dispatcher of the current thread for the
/// life of the thread. Runtime threads call it when they start.
pub fn make_thread_default(dispatch: &tracing::Dispatch) {
    // The guard would restore the earlier default when dropped; the runtime
    // thread keeps this default until it ends, so the guard is not dropped.
    std::mem::forget(tracing::dispatcher::set_default(dispatch));
}

/// The log setup of a task or handler child: the child host calls it once
/// with the worker's `--log-level`, before the boot. It sets every logger to
/// that level and starts the log bridge. It never fails: when the forwarder
/// cannot start, one line goes to stderr and the child keeps no log sink.
pub fn child_log_setup() -> ChildLogSetup {
    ChildLogSetup::Install(Box::new(|level: ChildLogLevel| {
        LEVELS.store_all(child_threshold(level), &[]);
        if let Err(failure) = started_log() {
            let _write_failure_ignored = writeln!(
                std::io::stderr(),
                "horsies: the log forwarder of this child could not start: {}",
                failure.cause
            );
        }
    }))
}

/// The threshold of a child's log level. Pure.
fn child_threshold(level: ChildLogLevel) -> LogThreshold {
    match level {
        ChildLogLevel::Debug => LogThreshold::Debug,
        ChildLogLevel::Info => LogThreshold::Info,
        ChildLogLevel::Warning => LogThreshold::Warning,
        ChildLogLevel::Error => LogThreshold::Error,
        ChildLogLevel::Critical => LogThreshold::Critical,
    }
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use super::*;

    fn record(level: RecordLevel) -> CoreLogRecord {
        CoreLogRecord::bridge(level, "message".to_owned())
    }

    #[test]
    fn only_a_record_that_entered_the_channel_is_counted() {
        let (sink, _receiver) = LogSink::new(1);
        sink.push(record(RecordLevel::Info));
        sink.push(record(RecordLevel::Debug));
        assert_eq!(sink.in_channel.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_drain_wait_ends_when_the_forwarder_handled_every_record() {
        let (sink, receiver) = LogSink::new(4);
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
        let (sink, _receiver) = LogSink::new(4);
        sink.push(record(RecordLevel::Error));
        sink.mark_forwarder_stopped();
        assert_eq!(
            sink.wait_for_drain(Duration::from_secs(5)),
            DrainOutcome::ForwarderStopped { left: 1 }
        );
    }

    #[test]
    fn a_drain_wait_ends_at_its_bound() {
        let (sink, _receiver) = LogSink::new(4);
        sink.mark_forwarder_running();
        sink.push(record(RecordLevel::Error));
        assert_eq!(
            sink.wait_for_drain(Duration::from_millis(20)),
            DrainOutcome::BoundReached { left: 1 }
        );
    }

    #[test]
    fn a_low_record_in_a_full_channel_is_dropped_and_counted() {
        let (sink, _receiver) = LogSink::new(1);
        sink.push(record(RecordLevel::Info));
        sink.push(record(RecordLevel::Debug));
        assert_eq!(
            sink.take_drop_counts(),
            DropCounts {
                dropped_low: 1,
                spilled_high: 0,
                abandoned: 0
            }
        );
        assert_eq!(
            sink.take_drop_counts(),
            DropCounts {
                dropped_low: 0,
                spilled_high: 0,
                abandoned: 0
            }
        );
    }

    #[test]
    fn a_high_record_in_a_full_channel_waits_then_goes_to_stderr() {
        let (sink, receiver) = LogSink::new(1);
        sink.push(record(RecordLevel::Info));
        let started = std::time::Instant::now();
        sink.push(record(RecordLevel::Warning));
        assert!(started.elapsed() >= HIGH_RECORD_WAIT);
        assert_eq!(
            sink.take_drop_counts(),
            DropCounts {
                dropped_low: 0,
                spilled_high: 1,
                abandoned: 0
            }
        );
        assert_eq!(
            receiver.try_recv().map(|kept| kept.level),
            Ok(RecordLevel::Info)
        );
    }

    #[test]
    fn a_high_record_takes_space_that_returns_while_it_waits() {
        let (sink, receiver) = LogSink::new(1);
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
    }

    #[test]
    fn the_stderr_line_holds_the_time_the_level_the_logger_and_the_fields() {
        let mut logged = record(RecordLevel::Info);
        logged.created = UNIX_EPOCH + Duration::from_micros(1_795_608_000_123_456);
        logged.message = "worker ready".to_owned();
        logged.fields = vec![
            RecordField {
                name: "queues",
                value: FieldValue::Text("default".to_owned()),
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
            logged.stderr_line(),
            "2026-11-25T12:00:00.123456Z INFO horsies.bridge worker ready queues=default \
             bound=10000 retry=false"
        );
    }

    #[test]
    fn the_stderr_only_switch_never_turns_back() {
        let (sink, _receiver) = LogSink::new(1);
        assert!(!sink.stderr_only());
        sink.switch_to_stderr();
        sink.switch_to_stderr();
        assert!(sink.stderr_only());
    }

    #[test]
    fn each_child_level_has_its_threshold() {
        assert_eq!(child_threshold(ChildLogLevel::Debug), LogThreshold::Debug);
        assert_eq!(child_threshold(ChildLogLevel::Info), LogThreshold::Info);
        assert_eq!(
            child_threshold(ChildLogLevel::Warning),
            LogThreshold::Warning
        );
        assert_eq!(child_threshold(ChildLogLevel::Error), LogThreshold::Error);
        assert_eq!(
            child_threshold(ChildLogLevel::Critical),
            LogThreshold::Critical
        );
    }
}

//! The log bridge: core `tracing` events to Python `logging`.
//!
//! - The subscriber checks each event against the level snapshot, formats
//!   it, and pushes one record into a bounded channel. A core thread never
//!   waits for the GIL to log.
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
//!
//! The subscriber is the default dispatcher of the runtime threads and of
//! every bridged operation; it is never the process-global default, so it
//! does not take the place of another library's subscriber.

mod forwarder;
mod levels;
mod names;
mod subscriber;

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Largest message of one record, in bytes. A longer message is cut at a
/// character boundary and marked.
pub const MAX_RECORD_MESSAGE_BYTES: usize = 16 * 1024;

/// The mark at the end of a cut message.
pub const CUT_MARK: &str = " [message cut]";

/// Where in the source an event was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordSource {
    Known { file: &'static str, line: u32 },
    Unknown,
}

/// One core log record on its way to Python.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreLogRecord {
    pub logger: LoggerName,
    pub level: RecordLevel,
    pub message: String,
    pub source: RecordSource,
    pub created: SystemTime,
    pub thread_name: String,
}

impl CoreLogRecord {
    /// A record of the bridge itself, created now on this thread.
    pub fn bridge(level: RecordLevel, message: String) -> Self {
        Self {
            logger: LoggerName::Bridge,
            level,
            message,
            source: RecordSource::Unknown,
            created: SystemTime::now(),
            thread_name: current_thread_name(),
        }
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

/// The sending side of the channel, the level snapshot and the counters.
pub struct LogSink {
    sender: SyncSender<CoreLogRecord>,
    levels: LevelSnapshot,
    dropped_low: AtomicU64,
    spilled_high: AtomicU64,
}

impl LogSink {
    /// Whether Python would handle a record of `level` on `logger`.
    pub fn enabled(&self, logger: LoggerName, level: RecordLevel) -> bool {
        self.levels.enabled(logger, level)
    }

    /// Push one record. Never waits for the GIL; a WARNING or ERROR record
    /// in a full channel waits up to [`HIGH_RECORD_WAIT`].
    pub fn push(&self, record: CoreLogRecord) {
        match self.sender.try_send(record) {
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
            match self.sender.try_send(waiting) {
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

    /// Write a record that cannot reach Python to stderr, with the counts.
    fn spill(&self, record: &CoreLogRecord) {
        let _write_failure_ignored = writeln!(
            std::io::stderr(),
            "{} {:?} {} (horsies log channel full or closed; dropped DEBUG/INFO records: {}, \
             WARNING/ERROR records written to stderr: {})",
            record.logger.python_name(),
            record.level,
            record.message,
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
        match self.sink.enabled(LoggerName::Bridge, level) {
            true => self.sink.push(CoreLogRecord::bridge(level, message)),
            false => {}
        }
    }

    /// Copy Python's effective levels into the snapshot. The caller is
    /// attached.
    pub fn refresh_levels(&self, py: Python<'_>) -> PyResult<()> {
        self.sink.refresh_levels(py)
    }
}

/// Make `dispatch` the default dispatcher of the current thread for the
/// life of the thread. Runtime threads call it when they start.
pub fn make_thread_default(dispatch: &tracing::Dispatch) {
    // The guard would restore the earlier default when dropped; the runtime
    // thread keeps this default until it ends, so the guard is not dropped.
    std::mem::forget(tracing::dispatcher::set_default(dispatch));
}

/// Cut `text` to at most `limit` bytes at a character boundary, and mark
/// the cut.
pub fn cut_to_limit(text: &mut String, limit: usize) {
    if text.len() <= limit {
        return;
    }
    // A UTF-8 character is at most 4 bytes, so a boundary is at most 3 bytes
    // below `limit`.
    let boundary = (limit.saturating_sub(3)..=limit)
        .rev()
        .find(|position| text.is_char_boundary(*position))
        .unwrap_or(0);
    text.truncate(boundary);
    text.push_str(CUT_MARK);
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
        };
        (sink, receiver)
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
    fn a_long_message_is_cut_at_a_character_boundary() {
        let mut text = "é".repeat(10);
        cut_to_limit(&mut text, 5);
        assert_eq!(text, format!("éé{CUT_MARK}"));
        let mut short = "short".to_owned();
        cut_to_limit(&mut short, 5);
        assert_eq!(short, "short");
    }
}

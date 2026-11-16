//! The forwarder thread: the one thread that hands core records to Python
//! `logging`.
//!
//! It takes the GIL once per batch, never while it holds a core lock. It
//! ends when the channel closes or when the interpreter can no longer be
//! attached (shutdown); records it then holds go to stderr. A panic in the
//! forwarder is a panic in a core background task: the core stops in this
//! process.

use std::io::Write;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use pyo3::prelude::*;
use pyo3::types::PyTuple;

use super::names::LoggerName;
use super::{CoreLogRecord, DropCounts, LogSink, RecordLevel, RecordSource};
use crate::process_core::{stop_after_background_panic, take_panic_cause, PanicReport};

/// How often the forwarder reads Python's effective levels again.
pub const LEVEL_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Most records handed to Python per GIL acquisition.
pub const FORWARD_BATCH: usize = 256;

/// Name of the forwarder thread.
pub const FORWARDER_THREAD_NAME: &str = "horsies-log-forwarder";

/// Start the forwarder thread. The thread is detached: it runs for the life
/// of the process.
pub fn start_forwarder(
    records: Receiver<CoreLogRecord>,
    sink: Arc<LogSink>,
) -> std::io::Result<()> {
    let _detached = std::thread::Builder::new()
        .name(FORWARDER_THREAD_NAME.to_owned())
        .spawn(move || run(&records, &sink))?;
    Ok(())
}

/// What one wait on the channel gave.
enum Received {
    Records(Vec<CoreLogRecord>),
    RefreshDue,
    Closed,
}

/// The service loop. It ends only when the channel closes, the interpreter
/// is gone, or the forwarder panics.
fn run(records: &Receiver<CoreLogRecord>, sink: &LogSink) {
    let mut loggers = LoggerCache::new();
    let mut refresh_at = Instant::now();
    loop {
        let wait = refresh_at.saturating_duration_since(Instant::now());
        let batch = match receive(records, wait) {
            Received::Records(batch) => batch,
            Received::RefreshDue => Vec::new(),
            Received::Closed => return,
        };
        let refresh_due = Instant::now() >= refresh_at;
        let attached = std::panic::catch_unwind(AssertUnwindSafe(|| {
            Python::try_attach(|py| forward_batch(py, &mut loggers, sink, &batch, refresh_due))
        }));
        match attached {
            Ok(Some(())) => {
                if refresh_due {
                    refresh_at = Instant::now() + LEVEL_REFRESH_INTERVAL;
                }
            }
            Ok(None) => {
                spill_all(&batch, records);
                return;
            }
            Err(payload) => {
                stop_after_background_panic(PanicReport {
                    subject: format!("thread {FORWARDER_THREAD_NAME}"),
                    cause: take_panic_cause(payload.as_ref()),
                });
                spill_all(&batch, records);
                return;
            }
        }
    }
}

/// Wait up to `wait` for a record, then take up to [`FORWARD_BATCH`].
fn receive(records: &Receiver<CoreLogRecord>, wait: Duration) -> Received {
    let first = match records.recv_timeout(wait) {
        Ok(record) => record,
        Err(RecvTimeoutError::Timeout) => return Received::RefreshDue,
        Err(RecvTimeoutError::Disconnected) => return Received::Closed,
    };
    let mut batch = Vec::with_capacity(FORWARD_BATCH);
    batch.push(first);
    while batch.len() < FORWARD_BATCH {
        match records.try_recv() {
            Ok(record) => batch.push(record),
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
        }
    }
    Received::Records(batch)
}

/// Hand one batch to Python, then report the drop counts. A failure of one
/// record goes to `sys.unraisablehook` and the batch goes on.
fn forward_batch(
    py: Python<'_>,
    loggers: &mut LoggerCache,
    sink: &LogSink,
    batch: &[CoreLogRecord],
    refresh_due: bool,
) {
    if refresh_due {
        if let Err(error) = refresh_levels(py, loggers, sink) {
            error.write_unraisable(py, None);
        }
    }
    for record in batch {
        if let Err(error) = forward_record(py, loggers, record) {
            error.write_unraisable(py, None);
        }
    }
    let DropCounts {
        dropped_low,
        spilled_high,
    } = sink.take_drop_counts();
    if dropped_low == 0 && spilled_high == 0 {
        return;
    }
    let report = CoreLogRecord::bridge(
        RecordLevel::Warning,
        format!(
            "the core log channel was full: {dropped_low} DEBUG/INFO records were \
             dropped and {spilled_high} WARNING/ERROR records were written to stderr"
        ),
    );
    if let Err(error) = forward_record(py, loggers, &report) {
        error.write_unraisable(py, None);
    }
}

fn refresh_levels(py: Python<'_>, loggers: &mut LoggerCache, sink: &LogSink) -> PyResult<()> {
    for logger in LoggerName::ALL {
        let level: i64 = loggers
            .get(py, logger)?
            .call_method0("getEffectiveLevel")?
            .extract()?;
        sink.levels.store(logger, level);
    }
    Ok(())
}

/// Build a `logging.LogRecord` with the core's time and thread name and
/// hand it to its logger, if the logger is enabled for its level.
fn forward_record(
    py: Python<'_>,
    loggers: &mut LoggerCache,
    record: &CoreLogRecord,
) -> PyResult<()> {
    let logger = loggers.get(py, record.logger)?;
    let level = record.level.python_level();
    if !logger.call_method1("isEnabledFor", (level,))?.is_truthy()? {
        return Ok(());
    }
    let (path, line) = match record.source {
        RecordSource::Known { file, line } => (file, line),
        RecordSource::Unknown => ("", 0),
    };
    // makeRecord(name, level, fn, lno, msg, args, exc_info): with empty
    // `args`, Python returns `msg` as it is, so `%` in a message is safe.
    let log_record = logger.call_method1(
        "makeRecord",
        (
            record.logger.python_name(),
            level,
            path,
            line,
            record.message.as_str(),
            PyTuple::empty(py),
            py.None(),
        ),
    )?;
    let created = record
        .created
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    log_record.setattr("created", created.as_secs_f64())?;
    log_record.setattr("msecs", f64::from(created.subsec_millis()))?;
    log_record.setattr("threadName", record.thread_name.as_str())?;
    let _handled = logger.call_method1("handle", (log_record,))?;
    Ok(())
}

/// Write the batch and everything left in the channel to stderr. Bounded by
/// the channel capacity.
fn spill_all(batch: &[CoreLogRecord], records: &Receiver<CoreLogRecord>) {
    let mut stderr = std::io::stderr();
    let left = std::iter::from_fn(|| records.try_recv().ok()).take(super::LOG_CHANNEL_CAPACITY);
    for record in batch.iter().cloned().chain(left) {
        let _write_failure_ignored = writeln!(
            stderr,
            "{} {:?} {}",
            record.logger.python_name(),
            record.level,
            record.message
        );
    }
}

/// The Python logger objects, fetched once each.
struct LoggerCache {
    loggers: [Option<Py<PyAny>>; LoggerName::COUNT],
}

impl LoggerCache {
    fn new() -> Self {
        Self {
            loggers: [const { None }; LoggerName::COUNT],
        }
    }

    fn get<'py>(&mut self, py: Python<'py>, logger: LoggerName) -> PyResult<Bound<'py, PyAny>> {
        if let Some(cached) = &self.loggers[logger.index()] {
            return Ok(cached.bind(py).clone());
        }
        let fetched = py
            .import("logging")?
            .call_method1("getLogger", (logger.python_name(),))?;
        self.loggers[logger.index()] = Some(fetched.clone().unbind());
        Ok(fetched)
    }
}

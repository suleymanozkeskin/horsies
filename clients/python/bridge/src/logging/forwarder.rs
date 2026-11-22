//! The forwarder thread: the one thread that hands core records to Python
//! `logging`.
//!
//! It takes the GIL once per batch, never while it holds a core lock. It
//! ends when the channel closes or when the interpreter can no longer be
//! attached (shutdown); records it then holds go to stderr. A panic in the
//! forwarder is a panic in a core background task: the process panic rule
//! applies (the core stops in this process, or a worker process aborts).
//! The forwarder counts every record it handles, so that an abort can wait
//! for the channel to empty.
//!
//! Each field of a record goes on its `LogRecord` as an attribute, as
//! `Logger.makeRecord` sets the keys of `extra`: a bool, an integer or a
//! float as a Python `bool`, `int` or `float`, any other value as a `str`.
//! A field whose name the record already uses gets [`TAKEN_NAME_PREFIX`]
//! (see [`set_field_attributes`]).

use std::io::Write;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3::IntoPyObjectExt;

use super::names::LoggerName;
use super::{
    CoreLogRecord, DropCounts, FieldValue, LogSink, RecordField, RecordLevel, RecordSource,
};
use crate::process_core::{background_task_panicked, take_panic_cause, PanicReport};

/// How often the forwarder reads Python's effective levels again.
pub const LEVEL_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Most records handed to Python per GIL acquisition.
pub const FORWARD_BATCH: usize = 256;

/// Name of the forwarder thread.
pub const FORWARDER_THREAD_NAME: &str = "horsies-log-forwarder";

/// The prefix of a field whose name the `LogRecord` already uses: the field
/// `name` goes on the record as `field_name`.
pub const TAKEN_NAME_PREFIX: &str = "field_";

/// The names that `Logger.makeRecord` refuses as keys of `extra` besides the
/// attributes that the record holds: a `Formatter` sets them.
const FORMATTER_ATTRIBUTES: [&str; 2] = ["message", "asctime"];

/// Start the forwarder thread. The thread is detached: it runs for the life
/// of the process.
pub fn start_forwarder(
    records: Receiver<CoreLogRecord>,
    sink: &Arc<LogSink>,
) -> std::io::Result<()> {
    sink.mark_forwarder_running();
    let thread_sink = Arc::clone(sink);
    let spawned = std::thread::Builder::new()
        .name(FORWARDER_THREAD_NAME.to_owned())
        .spawn(move || {
            run(&records, &thread_sink);
            thread_sink.mark_forwarder_stopped();
        });
    match spawned {
        Ok(_detached) => Ok(()),
        Err(error) => {
            sink.mark_forwarder_stopped();
            Err(error)
        }
    }
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
                sink.records_handled(batch.len());
                if refresh_due {
                    refresh_at = Instant::now() + LEVEL_REFRESH_INTERVAL;
                }
            }
            Ok(None) => {
                sink.records_handled(spill_all(&batch, records));
                return;
            }
            Err(payload) => {
                let cause = take_panic_cause(payload.as_ref());
                // Spill first and mark the forwarder stopped: under the abort
                // rule the panic call below does not return, and its drain
                // wait must not wait for this thread.
                sink.records_handled(spill_all(&batch, records));
                sink.mark_forwarder_stopped();
                background_task_panicked(PanicReport {
                    subject: format!("thread {FORWARDER_THREAD_NAME}"),
                    cause,
                });
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

/// Build a `logging.LogRecord` of `record` and hand it to its logger, if the
/// logger is enabled for its level.
fn forward_record(
    py: Python<'_>,
    loggers: &mut LoggerCache,
    record: &CoreLogRecord,
) -> PyResult<()> {
    let logger = loggers.get(py, record.logger)?;
    if !logger
        .call_method1("isEnabledFor", (record.level.python_level(),))?
        .is_truthy()?
    {
        return Ok(());
    }
    let log_record = log_record_of(py, &logger, record)?;
    let _handled = logger.call_method1("handle", (log_record,))?;
    Ok(())
}

/// The `logging.LogRecord` of `record`, made by `logger.makeRecord`, with
/// the core's time and thread name and each field as an attribute. Hands
/// the record to no handler.
fn log_record_of<'py>(
    py: Python<'py>,
    logger: &Bound<'py, PyAny>,
    record: &CoreLogRecord,
) -> PyResult<Bound<'py, PyAny>> {
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
            record.level.python_level(),
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
    set_field_attributes(py, &log_record, &record.fields)?;
    Ok(log_record)
}

/// Set each field on `log_record` as an attribute, in field order, as
/// `Logger.makeRecord` sets a key of `extra`: `record.__dict__[name] =
/// value`.
///
/// A name is taken when it is in [`FORMATTER_ATTRIBUTES`] or is a key of the
/// record's `__dict__`: an attribute of `LogRecord`, of a record factory, or
/// a field set before. `makeRecord` refuses such a key with `KeyError`;
/// here a field with a taken name goes on the record as
/// [`TAKEN_NAME_PREFIX`] and its name (`name` as `field_name`), and a field
/// whose prefixed name is taken too is left out.
fn set_field_attributes<'py>(
    py: Python<'py>,
    log_record: &Bound<'py, PyAny>,
    fields: &[RecordField],
) -> PyResult<()> {
    if fields.is_empty() {
        return Ok(());
    }
    let attributes = log_record.getattr("__dict__")?;
    for field in fields {
        let is_taken = |name: &str| -> PyResult<bool> {
            let reserved = FORMATTER_ATTRIBUTES.contains(&name);
            Ok(reserved || attributes.contains(name)?)
        };
        if let Some(name) = attribute_name(field.name, is_taken)? {
            attributes.set_item(name, field_object(py, &field.value)?)?;
        }
    }
    Ok(())
}

/// The attribute name of the field `name`: `name` itself, or
/// [`TAKEN_NAME_PREFIX`] and `name` when `is_taken` says `name` is taken.
/// `None` when both are taken. Asks `is_taken` at most twice.
fn attribute_name<E>(
    name: &str,
    mut is_taken: impl FnMut(&str) -> Result<bool, E>,
) -> Result<Option<String>, E> {
    if !is_taken(name)? {
        return Ok(Some(name.to_owned()));
    }
    let prefixed = format!("{TAKEN_NAME_PREFIX}{name}");
    match is_taken(&prefixed)? {
        true => Ok(None),
        false => Ok(Some(prefixed)),
    }
}

/// The Python object of a field value: `bool`, `int`, `float` or `str`.
fn field_object<'py>(py: Python<'py>, value: &FieldValue) -> PyResult<Bound<'py, PyAny>> {
    match value {
        FieldValue::Bool(value) => (*value).into_bound_py_any(py),
        FieldValue::Signed(value) => (*value).into_bound_py_any(py),
        FieldValue::Unsigned(value) => (*value).into_bound_py_any(py),
        FieldValue::Float(value) => (*value).into_bound_py_any(py),
        FieldValue::Text(value) => value.as_str().into_bound_py_any(py),
    }
}

/// Write the batch and everything left in the channel to stderr, and return
/// how many records were written. Bounded by the channel capacity.
fn spill_all(batch: &[CoreLogRecord], records: &Receiver<CoreLogRecord>) -> usize {
    let mut stderr = std::io::stderr();
    let left = std::iter::from_fn(|| records.try_recv().ok()).take(super::LOG_CHANNEL_CAPACITY);
    let mut written = 0;
    for record in batch.iter().cloned().chain(left) {
        let _write_failure_ignored = writeln!(
            stderr,
            "{} {:?} {}{}",
            record.logger.python_name(),
            record.level,
            record.message,
            record.fields_text()
        );
        written += 1;
    }
    written
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

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use pyo3::types::{PyBool, PyFloat, PyInt, PyString};

    use super::*;

    /// `is_taken` over a fixed list of taken names.
    fn taken_among<'a>(names: &'a [&'a str]) -> impl FnMut(&str) -> Result<bool, Infallible> + 'a {
        move |name| Ok(names.contains(&name))
    }

    /// The `LogRecord` of a WARNING record of `horsies.monitoring` with
    /// `fields`.
    fn log_record_with(py: Python<'_>, fields: Vec<RecordField>) -> PyResult<Bound<'_, PyAny>> {
        let mut record = CoreLogRecord::bridge(RecordLevel::Warning, "the list is cut".to_owned());
        record.logger = LoggerName::Monitoring;
        record.fields = fields;
        let logger = LoggerCache::new().get(py, LoggerName::Monitoring)?;
        log_record_of(py, &logger, &record)
    }

    fn field(name: &'static str, value: FieldValue) -> RecordField {
        RecordField { name, value }
    }

    #[test]
    fn a_free_name_is_the_attribute_name() {
        assert_eq!(
            attribute_name("read", taken_among(&["name"])),
            Ok(Some("read".to_owned()))
        );
    }

    #[test]
    fn a_taken_name_gets_the_prefix() {
        assert_eq!(
            attribute_name("name", taken_among(&["name"])),
            Ok(Some("field_name".to_owned()))
        );
    }

    #[test]
    fn a_name_taken_with_its_prefix_has_no_attribute_name() {
        assert_eq!(
            attribute_name("name", taken_among(&["name", "field_name"])),
            Ok(None)
        );
    }

    #[test]
    fn each_field_is_an_attribute_of_the_log_record_with_its_python_type() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let log_record = log_record_with(
                py,
                vec![
                    field("read", FieldValue::Text("schedule state".to_owned())),
                    field("bound", FieldValue::Unsigned(10_000)),
                    field("signed", FieldValue::Signed(-3)),
                    field("ratio", FieldValue::Float(0.5)),
                    field("retried", FieldValue::Bool(true)),
                ],
            )?;

            assert_eq!(
                log_record.call_method0("getMessage")?.extract::<String>()?,
                "the list is cut"
            );
            let read = log_record.getattr("read")?;
            assert!(read.is_exact_instance_of::<PyString>());
            assert_eq!(read.extract::<String>()?, "schedule state");
            let bound = log_record.getattr("bound")?;
            assert!(bound.is_exact_instance_of::<PyInt>());
            assert_eq!(bound.extract::<u64>()?, 10_000);
            let signed = log_record.getattr("signed")?;
            assert!(signed.is_exact_instance_of::<PyInt>());
            assert_eq!(signed.extract::<i64>()?, -3);
            let ratio = log_record.getattr("ratio")?;
            assert!(ratio.is_exact_instance_of::<PyFloat>());
            assert_eq!(ratio.extract::<f64>()?, 0.5);
            let retried = log_record.getattr("retried")?;
            assert!(retried.is_exact_instance_of::<PyBool>());
            assert!(retried.extract::<bool>()?);
            Ok(())
        })
        .expect("the log record is built");
    }

    #[test]
    fn a_field_with_a_name_that_the_log_record_uses_goes_under_the_prefix() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let log_record = log_record_with(
                py,
                vec![
                    field("name", FieldValue::Text("nightly.flow".to_owned())),
                    field("created", FieldValue::Signed(7)),
                    field("asctime", FieldValue::Text("not a time".to_owned())),
                ],
            )?;

            assert_eq!(
                log_record.getattr("name")?.extract::<String>()?,
                "horsies.monitoring"
            );
            assert_eq!(
                log_record.getattr("field_name")?.extract::<String>()?,
                "nightly.flow"
            );
            assert!(log_record
                .getattr("created")?
                .is_exact_instance_of::<PyFloat>());
            assert_eq!(log_record.getattr("field_created")?.extract::<i64>()?, 7);
            assert!(!log_record.hasattr("asctime")?);
            assert_eq!(
                log_record.getattr("field_asctime")?.extract::<String>()?,
                "not a time"
            );
            Ok(())
        })
        .expect("the log record is built");
    }

    #[test]
    fn a_field_whose_prefixed_name_an_earlier_field_took_is_left_out() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let log_record = log_record_with(
                py,
                vec![
                    field("field_name", FieldValue::Text("first".to_owned())),
                    field("name", FieldValue::Text("second".to_owned())),
                ],
            )?;

            assert_eq!(
                log_record.getattr("field_name")?.extract::<String>()?,
                "first"
            );
            assert_eq!(
                log_record.getattr("name")?.extract::<String>()?,
                "horsies.monitoring"
            );
            let values = log_record.getattr("__dict__")?.call_method0("values")?;
            assert!(!values.contains("second")?);
            Ok(())
        })
        .expect("the log record is built");
    }
}

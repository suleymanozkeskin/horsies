//! The JavaScript sink of the core log records.
//!
//! - The package sets the sink with `setLogSink(sink)` and takes it away with
//!   `resetLogSink()`; without one, the forwarder writes one stderr line per
//!   record. A change applies to the next batch.
//! - The sink is a weak threadsafe function: it does not keep the event loop
//!   alive, so a process with an idle core can exit.
//! - The forwarder hands one batch at a time and waits for the sink's
//!   answer, so the drain before an abort counts the records that the sink
//!   took. The wait ends at the answer, at [`SINK_ANSWER_BOUND`], or when the
//!   forwarder switched to stderr (the environment ends, or the process
//!   aborts from the JavaScript thread).
//! - At the bound, a batch that the JavaScript thread has not taken yet is
//!   abandoned: its records count as dropped, and a late call gives the sink
//!   an empty batch, so no record arrives twice. At the stderr switch, such a
//!   batch goes to stderr. A batch that the sink holds already is the sink's;
//!   the forwarder goes on.
//! - The package's wrapper of the user's sink never throws: it answers
//!   `true` when the sink took the batch and `false` when the sink threw.
//!   A batch that the sink did not take goes to stderr.

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use horsies_core::frame_protocol::LOOP_BLOCK_LIMIT;
use napi::bindgen_prelude::{sys, BigInt, Function, ToNapiValue};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::Status;
use napi_derive::napi;

use super::bridge::{CoreLogRecord, FieldValue, LogSink, RecordSource};
use super::levels::RecordLevel;
use super::names::LoggerName;
use crate::call::guard::sync_call;
use crate::call::node_api::{call_status, CallStatus};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::values::boundary::epoch_micros_of;

/// Calls of the sink that may wait in its queue. The forwarder waits for
/// each answer before its next call, so one is enough.
const SINK_QUEUE_BOUND: usize = 1;

/// Tries of a call that meets a full queue.
const QUEUE_FULL_ATTEMPTS: u32 = 100;

/// The pause between two tries of a call that met a full queue.
const QUEUE_FULL_PAUSE: Duration = Duration::from_millis(10);

/// How often the forwarder reads the stderr switch and the bound while it
/// waits for the sink's answer.
const ANSWER_POLL: Duration = Duration::from_millis(50);

/// The longest wait for the sink's answer: the core's bound of an event loop
/// that does not turn. A JavaScript thread that does not take the batch
/// within it is blocked.
pub const SINK_ANSWER_BOUND: Duration = LOOP_BLOCK_LIMIT;

/// The threadsafe function of the sink: weak, never throws into Node-API.
type JsLogSink =
    ThreadsafeFunction<SinkBatch, bool, SinkBatch, Status, false, true, SINK_QUEUE_BOUND>;

/// The phase of one batch: the forwarder waits for the JavaScript thread,
/// the sink holds it, or the forwarder abandoned it.
const BATCH_WAITING: u8 = 0;
const BATCH_IN_SINK: u8 = 1;
const BATCH_ABANDONED: u8 = 2;

/// Who has one batch: the JavaScript thread or the forwarder decides it
/// once, with one compare-and-swap.
pub struct BatchState(AtomicU8);

/// What the JavaScript thread gives the sink.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkTake {
    Records,
    /// The forwarder abandoned the batch: the sink gets no record.
    Empty,
}

/// How the forwarder's abandon of a batch ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Abandon {
    /// The JavaScript thread had not taken the batch: it never will.
    Abandoned,
    /// The sink holds the batch already.
    InSink,
}

impl BatchState {
    fn new() -> Self {
        Self(AtomicU8::new(BATCH_WAITING))
    }

    /// The JavaScript thread takes the batch for the sink, unless the
    /// forwarder abandoned it.
    pub fn take_for_sink(&self) -> SinkTake {
        match self.0.compare_exchange(
            BATCH_WAITING,
            BATCH_IN_SINK,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => SinkTake::Records,
            Err(_abandoned) => SinkTake::Empty,
        }
    }

    /// The forwarder abandons the batch, unless the sink holds it.
    pub fn abandon(&self) -> Abandon {
        match self.0.compare_exchange(
            BATCH_WAITING,
            BATCH_ABANDONED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Abandon::Abandoned,
            Err(_in_sink) => Abandon::InSink,
        }
    }
}

/// One batch for the JavaScript sink, with its phase.
#[derive(Clone)]
pub struct SinkBatch {
    records: Vec<LogRecordOut>,
    state: Arc<BatchState>,
}

impl ToNapiValue for SinkBatch {
    /// The array of records, or an empty array when the forwarder abandoned
    /// the batch. Runs on the JavaScript thread, before the sink.
    unsafe fn to_napi_value(env: sys::napi_env, batch: Self) -> napi::Result<sys::napi_value> {
        let records = match batch.state.take_for_sink() {
            SinkTake::Records => batch.records,
            SinkTake::Empty => Vec::new(),
        };
        // SAFETY: `env` is the live environment of the JavaScript thread that
        // calls `to_napi_value`, as `Vec::to_napi_value` requires.
        unsafe { Vec::<LogRecordOut>::to_napi_value(env, records) }
    }
}

/// The sink in force.
enum SinkTarget {
    Stderr,
    JavaScript(Arc<JsLogSink>),
}

static TARGET: Mutex<SinkTarget> = Mutex::new(SinkTarget::Stderr);

/// One core log record, as the JavaScript sink gets it.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct LogRecordOut {
    pub logger: LoggerName,
    pub level: RecordLevel,
    pub message: String,
    pub fields: Vec<LogFieldOut>,
    /// When the event was written, in epoch microseconds.
    pub epoch_micros: BigInt,
    /// The name of the thread that wrote the event.
    pub thread: String,
    pub source: LogSourceOut,
}

/// One field of a record other than its message.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct LogFieldOut {
    pub name: String,
    pub value: LogFieldValueOut,
}

/// The value of one field, as the event recorded it.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum LogFieldValueOut {
    Flag { value: bool },
    Integer { value: BigInt },
    Float { value: f64 },
    Text { text: String },
}

/// Where in the source an event was written.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogSourceOut {
    Known { file: String, line: u32 },
    Unknown,
}

impl LogRecordOut {
    /// The JavaScript form of `record`. Pure.
    pub fn of(record: &CoreLogRecord) -> Self {
        Self {
            logger: record.logger,
            level: record.level,
            message: record.message.clone(),
            fields: record
                .fields
                .iter()
                .map(|field| LogFieldOut {
                    name: field.name.to_owned(),
                    value: field_value(&field.value),
                })
                .collect(),
            epoch_micros: epoch_micros_of(record.created.into()),
            thread: record.thread_name.clone(),
            source: match record.source {
                RecordSource::Known { file, line } => LogSourceOut::Known {
                    file: file.to_owned(),
                    line,
                },
                RecordSource::Unknown => LogSourceOut::Unknown,
            },
        }
    }
}

/// The JavaScript form of one field value. Pure.
fn field_value(value: &FieldValue) -> LogFieldValueOut {
    match value {
        FieldValue::Bool(flag) => LogFieldValueOut::Flag { value: *flag },
        FieldValue::Signed(number) => LogFieldValueOut::Integer {
            value: BigInt::from(*number),
        },
        FieldValue::Unsigned(number) => LogFieldValueOut::Integer {
            value: BigInt::from(*number),
        },
        FieldValue::Float(number) => LogFieldValueOut::Float { value: *number },
        FieldValue::Text(text) => LogFieldValueOut::Text { text: text.clone() },
    }
}

/// Set the JavaScript sink of the core log records. Later batches go to it.
/// Starts nothing.
#[napi(
    ts_args_type = "sink: (records: Array<LogRecordOut>) => boolean",
    ts_return_type = "NativeEnvelope<undefined>"
)]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn set_log_sink(sink: Function<SinkBatch, bool>) -> Envelope<()> {
    sync_call("set_log_sink", || {
        let built: napi::Result<JsLogSink> = sink
            .build_threadsafe_function::<SinkBatch>()
            .callee_handled::<false>()
            .weak::<true>()
            .max_queue_size::<SINK_QUEUE_BOUND>()
            .build();
        match built {
            Ok(threadsafe) => {
                *TARGET.lock().unwrap_or_else(PoisonError::into_inner) =
                    SinkTarget::JavaScript(Arc::new(threadsafe));
                Envelope::Value(())
            }
            Err(error) => Envelope::Thrown(NativeThrown::ClientDefect {
                subject: "setLogSink".to_owned(),
                rule: format!("Node-API refused the threadsafe function of the sink: {error}"),
            }),
        }
    })
}

/// Take the JavaScript sink away: later batches go to stderr, one line per
/// record. Starts nothing.
#[napi(ts_return_type = "NativeEnvelope<undefined>")]
pub fn reset_log_sink() -> Envelope<()> {
    sync_call("reset_log_sink", || {
        *TARGET.lock().unwrap_or_else(PoisonError::into_inner) = SinkTarget::Stderr;
        Envelope::Value(())
    })
}

/// How the hand of one batch to the JavaScript sink ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Handed {
    /// The sink took the batch.
    Taken,
    /// No JavaScript sink is set, or the forwarder writes to stderr only.
    NoJavaScriptSink,
    /// No answer came within [`SINK_ANSWER_BOUND`], and the JavaScript thread
    /// had not taken the batch: its records count as dropped.
    Abandoned,
    /// No answer came within [`SINK_ANSWER_BOUND`] or before the stderr
    /// switch, and the sink holds the batch: the records are the sink's, and
    /// the forwarder goes on.
    InSinkUnanswered,
    /// The sink did not take the batch, so it goes to stderr.
    NotTaken(NotTaken),
}

/// Why the sink did not take a batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotTaken {
    /// The user's sink threw; the package's wrapper answered `false`.
    SinkThrew,
    /// The answer does not read as a boolean (a throw out of the wrapper
    /// arrives here too): a defect of the package.
    AnswerUnreadable { cause: String },
    /// Node-API dropped the call with its answer channel.
    CallDropped,
    /// The forwarder switched to stderr while it waited, and the JavaScript
    /// thread had not taken the batch: the environment ends, or the process
    /// aborts from the JavaScript thread.
    ProcessEnds,
    /// The threadsafe function of the sink is closing.
    EnvironmentClosing,
    /// Node-API refused the call with `status`.
    CallRefused { status: Status },
    /// The queue of the sink stayed full for every try.
    QueueStayedFull,
}

impl fmt::Display for NotTaken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SinkThrew => formatter.write_str("the JavaScript sink threw"),
            Self::AnswerUnreadable { cause } => {
                write!(
                    formatter,
                    "the answer of the JavaScript sink does not read: {cause}"
                )
            }
            Self::CallDropped => formatter.write_str("Node-API dropped the call of the sink"),
            Self::ProcessEnds => {
                formatter.write_str("the process ends and the JavaScript thread does not answer")
            }
            Self::EnvironmentClosing => {
                formatter.write_str("the JavaScript environment is closing")
            }
            Self::CallRefused { status } => {
                write!(formatter, "Node-API refused the call of the sink: {status}")
            }
            Self::QueueStayedFull => {
                formatter.write_str("the queue of the JavaScript sink stayed full")
            }
        }
    }
}

/// Hand `batch` to the JavaScript sink in force and wait for its answer,
/// at most [`SINK_ANSWER_BOUND`].
pub fn hand_batch(log: &LogSink, batch: &[CoreLogRecord]) -> Handed {
    let sink = match &*TARGET.lock().unwrap_or_else(PoisonError::into_inner) {
        SinkTarget::Stderr => return Handed::NoJavaScriptSink,
        SinkTarget::JavaScript(sink) => Arc::clone(sink),
    };
    let batch = SinkBatch {
        records: batch.iter().map(LogRecordOut::of).collect(),
        state: Arc::new(BatchState::new()),
    };
    let (sender, receiver) = std::sync::mpsc::sync_channel::<napi::Result<bool>>(1);
    for _attempt in 0..QUEUE_FULL_ATTEMPTS {
        let answer_sender = sender.clone();
        let status = sink.call_with_return_value(
            batch.clone(),
            ThreadsafeFunctionCallMode::NonBlocking,
            move |answer, _env| {
                // The forwarder may have stopped waiting; the answer then
                // has no reader.
                let _no_reader = answer_sender.send(answer);
                Ok(())
            },
        );
        match call_status(status) {
            CallStatus::Queued => {
                drop(sender);
                return wait_for_answer(log, &receiver, &batch.state, SINK_ANSWER_BOUND);
            }
            CallStatus::QueueFull => std::thread::sleep(QUEUE_FULL_PAUSE),
            CallStatus::Closing => {
                log.switch_to_stderr();
                return Handed::NotTaken(NotTaken::EnvironmentClosing);
            }
            CallStatus::Refused { status } => {
                return Handed::NotTaken(NotTaken::CallRefused { status })
            }
        }
    }
    Handed::NotTaken(NotTaken::QueueStayedFull)
}

/// Wait for the sink's answer to one call, at most `bound`; see the module
/// rules.
fn wait_for_answer(
    log: &LogSink,
    receiver: &Receiver<napi::Result<bool>>,
    state: &BatchState,
    bound: Duration,
) -> Handed {
    let started = Instant::now();
    loop {
        match receiver.recv_timeout(ANSWER_POLL) {
            Ok(Ok(true)) => return Handed::Taken,
            Ok(Ok(false)) => return Handed::NotTaken(NotTaken::SinkThrew),
            Ok(Err(error)) => {
                return Handed::NotTaken(NotTaken::AnswerUnreadable {
                    cause: error.to_string(),
                })
            }
            Err(RecvTimeoutError::Disconnected) => return Handed::NotTaken(NotTaken::CallDropped),
            Err(RecvTimeoutError::Timeout) => {
                match (log.stderr_only(), started.elapsed() >= bound) {
                    (true, _) => {
                        return match state.abandon() {
                            Abandon::Abandoned => Handed::NotTaken(NotTaken::ProcessEnds),
                            Abandon::InSink => Handed::InSinkUnanswered,
                        }
                    }
                    (false, true) => {
                        return match state.abandon() {
                            Abandon::Abandoned => Handed::Abandoned,
                            Abandon::InSink => Handed::InSinkUnanswered,
                        }
                    }
                    (false, false) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;
    use crate::logging::bridge::RecordField;

    /// A bound that a test passes quickly.
    const SHORT_BOUND: Duration = Duration::from_millis(20);

    /// A bound that no test reaches.
    const LONG_BOUND: Duration = Duration::from_secs(600);

    /// Wait for one answer that the test sends first.
    fn waited(answer: napi::Result<bool>) -> Handed {
        let (log, _records) = LogSink::new(1);
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        sender.send(answer).expect("the channel has room");
        wait_for_answer(&log, &receiver, &BatchState::new(), LONG_BOUND)
    }

    #[test]
    fn an_answer_true_is_taken() {
        assert_eq!(waited(Ok(true)), Handed::Taken);
    }

    #[test]
    fn an_answer_false_is_a_throw_of_the_sink() {
        assert_eq!(waited(Ok(false)), Handed::NotTaken(NotTaken::SinkThrew));
    }

    #[test]
    fn an_answer_that_does_not_read_keeps_its_cause() {
        assert_eq!(
            waited(Err(napi::Error::new(Status::PendingException, "thrown"))),
            Handed::NotTaken(NotTaken::AnswerUnreadable {
                cause: napi::Error::new(Status::PendingException, "thrown").to_string()
            })
        );
    }

    #[test]
    fn a_dropped_call_is_not_taken() {
        let (log, _records) = LogSink::new(1);
        let (sender, receiver) = std::sync::mpsc::sync_channel::<napi::Result<bool>>(1);
        drop(sender);
        assert_eq!(
            wait_for_answer(&log, &receiver, &BatchState::new(), LONG_BOUND),
            Handed::NotTaken(NotTaken::CallDropped)
        );
    }

    #[test]
    fn the_stderr_switch_ends_the_wait() {
        let (log, _records) = LogSink::new(1);
        let (_sender, receiver) = std::sync::mpsc::sync_channel::<napi::Result<bool>>(1);
        log.switch_to_stderr();
        assert_eq!(
            wait_for_answer(&log, &receiver, &BatchState::new(), LONG_BOUND),
            Handed::NotTaken(NotTaken::ProcessEnds)
        );
    }

    #[test]
    fn the_stderr_switch_leaves_a_batch_in_the_sink_to_the_sink() {
        let (log, _records) = LogSink::new(1);
        let (_sender, receiver) = std::sync::mpsc::sync_channel::<napi::Result<bool>>(1);
        let state = BatchState::new();
        assert_eq!(state.take_for_sink(), SinkTake::Records);
        log.switch_to_stderr();
        assert_eq!(
            wait_for_answer(&log, &receiver, &state, LONG_BOUND),
            Handed::InSinkUnanswered
        );
    }

    #[test]
    fn at_the_bound_a_batch_that_javascript_has_not_taken_is_abandoned() {
        let (log, _records) = LogSink::new(1);
        let (_sender, receiver) = std::sync::mpsc::sync_channel::<napi::Result<bool>>(1);
        let state = BatchState::new();
        assert_eq!(
            wait_for_answer(&log, &receiver, &state, SHORT_BOUND),
            Handed::Abandoned
        );
        assert_eq!(state.take_for_sink(), SinkTake::Empty);
    }

    #[test]
    fn at_the_bound_a_batch_that_the_sink_holds_stays_the_sinks() {
        let (log, _records) = LogSink::new(1);
        let (_sender, receiver) = std::sync::mpsc::sync_channel::<napi::Result<bool>>(1);
        let state = BatchState::new();
        assert_eq!(state.take_for_sink(), SinkTake::Records);
        assert_eq!(
            wait_for_answer(&log, &receiver, &state, SHORT_BOUND),
            Handed::InSinkUnanswered
        );
    }

    #[test]
    fn a_record_keeps_its_facts_and_each_field_its_type() {
        let mut record = CoreLogRecord::bridge(RecordLevel::Warning, "cut".to_owned());
        record.created = UNIX_EPOCH + Duration::from_micros(1_795_608_000_123_456);
        record.source = RecordSource::Known {
            file: "core/src/web.rs",
            line: 7,
        };
        record.fields = vec![
            RecordField {
                name: "retry",
                value: FieldValue::Bool(true),
            },
            RecordField {
                name: "bytes",
                value: FieldValue::Unsigned(u128::from(u64::MAX)),
            },
            RecordField {
                name: "delta",
                value: FieldValue::Signed(-3),
            },
            RecordField {
                name: "ratio",
                value: FieldValue::Float(0.5),
            },
            RecordField {
                name: "read",
                value: FieldValue::Text("schedules".to_owned()),
            },
        ];
        let out = LogRecordOut::of(&record);
        assert_eq!(out.logger, LoggerName::Bridge);
        assert_eq!(out.level, RecordLevel::Warning);
        assert_eq!(out.message, "cut");
        assert_eq!(out.epoch_micros, BigInt::from(1_795_608_000_123_456_i128));
        assert_eq!(
            out.source,
            LogSourceOut::Known {
                file: "core/src/web.rs".to_owned(),
                line: 7
            }
        );
        assert_eq!(
            out.fields
                .iter()
                .map(|field| field.value.clone())
                .collect::<Vec<_>>(),
            vec![
                LogFieldValueOut::Flag { value: true },
                LogFieldValueOut::Integer {
                    value: BigInt::from(u64::MAX)
                },
                LogFieldValueOut::Integer {
                    value: BigInt::from(-3_i128)
                },
                LogFieldValueOut::Float { value: 0.5 },
                LogFieldValueOut::Text {
                    text: "schedules".to_owned()
                },
            ]
        );
    }
}

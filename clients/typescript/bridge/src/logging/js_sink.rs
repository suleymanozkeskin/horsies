//! The JavaScript sink of the core log records.
//!
//! - The package sets the sink with `setLogSink(sink)` and takes it away with
//!   `resetLogSink()`; without one, the forwarder writes one stderr line per
//!   record. A change applies to the next batch.
//! - The sink is a weak threadsafe function: it does not keep the event loop
//!   alive, so a process with an idle core can exit.
//! - The forwarder hands one batch at a time and waits for the sink's
//!   answer, so the drain before an abort counts the records that the sink
//!   took. The wait ends when the sink answers, when the call is dropped (the
//!   environment ends), or when the forwarder switched to stderr (the
//!   environment ends, or the process aborts from the JavaScript thread).
//!   While the JavaScript thread is blocked, the wait goes on and new records
//!   follow the drop rule of the channel.
//! - The package's wrapper of the user's sink never throws: it answers
//!   `true` when the sink took the batch and `false` when the sink threw.
//!   A batch that the sink did not take goes to stderr.

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use napi::bindgen_prelude::{BigInt, Function};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::Status;
use napi_derive::napi;

use super::bridge::{CoreLogRecord, FieldValue, LogSink, RecordSource};
use super::levels::RecordLevel;
use super::names::LoggerName;
use crate::call::guard::sync_call;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::values::boundary::epoch_micros_of;

/// Calls of the sink that may wait in its queue. The forwarder waits for
/// each answer before its next call, so one is enough.
const SINK_QUEUE_BOUND: usize = 1;

/// Tries of a call that meets a full queue.
const QUEUE_FULL_ATTEMPTS: u32 = 100;

/// The pause between two tries of a call that met a full queue.
const QUEUE_FULL_PAUSE: Duration = Duration::from_millis(10);

/// How often the forwarder reads the stderr switch while it waits for the
/// sink's answer.
const ANSWER_POLL: Duration = Duration::from_millis(50);

/// The threadsafe function of the sink: weak, never throws into Node-API.
type JsLogSink = ThreadsafeFunction<
    Vec<LogRecordOut>,
    bool,
    Vec<LogRecordOut>,
    Status,
    false,
    true,
    SINK_QUEUE_BOUND,
>;

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
#[napi(ts_return_type = "NativeEnvelope<undefined>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn set_log_sink(sink: Function<Vec<LogRecordOut>, bool>) -> Envelope<()> {
    sync_call("set_log_sink", || {
        let built: napi::Result<JsLogSink> = sink
            .build_threadsafe_function::<Vec<LogRecordOut>>()
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
                rule: format!("the sink must be a function: {error}"),
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Handed {
    /// The sink took the batch.
    Taken,
    /// No JavaScript sink is set, or the forwarder writes to stderr only.
    NoJavaScriptSink,
    /// The sink did not take the batch, for `reason`.
    NotTaken { reason: &'static str },
}

/// Hand `batch` to the JavaScript sink in force and wait for its answer.
pub fn hand_batch(log: &LogSink, batch: &[CoreLogRecord]) -> Handed {
    let sink = match &*TARGET.lock().unwrap_or_else(PoisonError::into_inner) {
        SinkTarget::Stderr => return Handed::NoJavaScriptSink,
        SinkTarget::JavaScript(sink) => Arc::clone(sink),
    };
    let records: Vec<LogRecordOut> = batch.iter().map(LogRecordOut::of).collect();
    let (sender, receiver) = std::sync::mpsc::sync_channel::<napi::Result<bool>>(1);
    for _attempt in 0..QUEUE_FULL_ATTEMPTS {
        let answer_sender = sender.clone();
        let status = sink.call_with_return_value(
            records.clone(),
            ThreadsafeFunctionCallMode::NonBlocking,
            move |answer, _env| {
                // The forwarder may have stopped waiting; the answer then
                // has no reader.
                let _no_reader = answer_sender.send(answer);
                Ok(())
            },
        );
        match status {
            Status::Ok => {
                drop(sender);
                return wait_for_answer(log, &receiver);
            }
            Status::QueueFull => std::thread::sleep(QUEUE_FULL_PAUSE),
            Status::Closing => {
                log.switch_to_stderr();
                return Handed::NotTaken {
                    reason: "the JavaScript environment is closing",
                };
            }
            _refused => {
                return Handed::NotTaken {
                    reason: "Node-API refused the call of the JavaScript sink",
                }
            }
        }
    }
    Handed::NotTaken {
        reason: "the queue of the JavaScript sink stayed full",
    }
}

/// Wait for the sink's answer to one call; see the module rules.
fn wait_for_answer(log: &LogSink, receiver: &Receiver<napi::Result<bool>>) -> Handed {
    loop {
        match receiver.recv_timeout(ANSWER_POLL) {
            Ok(Ok(true)) => return Handed::Taken,
            Ok(Ok(false)) => {
                return Handed::NotTaken {
                    reason: "the JavaScript sink threw",
                }
            }
            Ok(Err(_not_a_boolean)) => {
                return Handed::NotTaken {
                    reason: "the JavaScript sink answered a value that is not a boolean",
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Handed::NotTaken {
                    reason: "the JavaScript environment dropped the call",
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if log.stderr_only() {
                    return Handed::NotTaken {
                        reason: "the process ends and the JavaScript thread does not answer",
                    };
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

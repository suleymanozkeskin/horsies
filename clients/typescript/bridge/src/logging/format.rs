//! The format of the stderr lines of the core log records of this process:
//! one line of text per record (the default), or one JSON object per line.
//!
//! - The package sets the format of the app (`HorsiesConfig.logFormat`) in
//!   the worker, the scheduler, `check` and each worker child
//!   (`setLogFormat`). A change applies to the lines of later writes.
//! - The format applies to every line that the bridge writes to stderr for a
//!   record: the default stderr sink, a batch that the JavaScript sink did
//!   not take, and a record that cannot enter the channel. A JavaScript sink
//!   gets the records, not the lines, so the format does not apply to it.
//! - A JSON line has the keys of Python's `JsonLogsFormatter`, in its order,
//!   and the same keys in every record ([`json_line`]). The output is
//!   ASCII: each other character is a `\u` escape, as Python's `json.dumps`
//!   writes it.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::SystemTime;

use chrono::{DateTime, SecondsFormat, Utc};
use napi_derive::napi;

use super::bridge::{CoreLogRecord, FieldValue};
use super::levels::RecordLevel;
use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;

/// The format of the stderr lines of the core log records.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormatKind {
    /// One line of text per record: the time, the level, the logger, the
    /// message and each field as ` name=value` (`CoreLogRecord::stderr_line`).
    TextLogs,
    /// One JSON object per line, with the keys of [`json_line`].
    JsonLogs,
}

/// The stored code of [`LogFormatKind::TextLogs`].
const TEXT_LOGS_CODE: u8 = 0;

/// The stored code of [`LogFormatKind::JsonLogs`].
const JSON_LOGS_CODE: u8 = 1;

impl LogFormatKind {
    const fn code(self) -> u8 {
        match self {
            Self::TextLogs => TEXT_LOGS_CODE,
            Self::JsonLogs => JSON_LOGS_CODE,
        }
    }

    /// The format of a stored code. Only [`Self::code`] stores a code, so
    /// another value is a defect of this module.
    fn of_code(code: u8) -> Self {
        match code {
            TEXT_LOGS_CODE => Self::TextLogs,
            JSON_LOGS_CODE => Self::JsonLogs,
            other => panic!("the stored log format code {other} is not a code of LogFormatKind"),
        }
    }
}

/// The format of this process. Text until the package sets another.
static FORMAT: AtomicU8 = AtomicU8::new(TEXT_LOGS_CODE);

/// The keys of a JSON line that take the value of the record field of the
/// same name, or `null` when the record has no such field (Python's
/// `JSON_LOG_CONTEXT_FIELDS`).
const CONTEXT_KEYS: [&str; 5] = ["task_id", "task_name", "attempt", "trace_id", "span_id"];

/// Set the format of the stderr lines of later core log records. Starts
/// nothing.
#[napi(ts_return_type = "NativeEnvelope<undefined>")]
pub fn set_log_format(format: LogFormatKind) -> Envelope<()> {
    sync_call("set_log_format", move || {
        store_format(format);
        Envelope::Value(())
    })
}

fn store_format(format: LogFormatKind) {
    FORMAT.store(format.code(), Ordering::SeqCst);
}

/// The format of this process now.
pub fn current_format() -> LogFormatKind {
    LogFormatKind::of_code(FORMAT.load(Ordering::SeqCst))
}

/// The stderr line of `record` in the format of this process, with no
/// newline.
pub fn record_line(record: &CoreLogRecord) -> String {
    line_in(current_format(), record)
}

/// The stderr line of `record` in `format`, with no newline. Pure but for
/// the read of the process id.
pub fn line_in(format: LogFormatKind, record: &CoreLogRecord) -> String {
    match format {
        LogFormatKind::TextLogs => record.stderr_line(),
        LogFormatKind::JsonLogs => json_line(record, std::process::id()),
    }
}

/// The stderr text of a notice of the bridge that is no core record (for
/// example, why a batch goes to stderr), with no newline: the text as it is,
/// or in the JSON format the line of a WARNING record of the bridge logger
/// with the text as its message.
pub fn notice_line(text: String) -> String {
    match current_format() {
        LogFormatKind::TextLogs => text,
        LogFormatKind::JsonLogs => json_line(
            &CoreLogRecord::bridge(RecordLevel::Warning, text),
            std::process::id(),
        ),
    }
}

/// The JSON line of `record`, written by the process `process`, with no
/// newline: the keys `ts`, `level`, `logger`, `message`, `process`, then
/// the context keys ([`CONTEXT_KEYS`]), then `exc`. A context key holds the
/// value of the first record field of its name: a text as a JSON string, an
/// integer as a JSON number, a flag as a JSON flag, and a float as the
/// string of its text. `exc` is always `null`: a core record carries no
/// exception. Pure.
pub fn json_line(record: &CoreLogRecord, process: u32) -> String {
    let mut line = String::with_capacity(256);
    line.push('{');
    push_member(&mut line, "ts", &json_text(&ts_text(record.created)));
    push_member(&mut line, "level", &json_text(record.level.text()));
    push_member(&mut line, "logger", &json_text(record.logger.dotted_name()));
    push_member(&mut line, "message", &json_text(&record.message));
    push_member(&mut line, "process", &process.to_string());
    for key in CONTEXT_KEYS {
        push_member(&mut line, key, &context_value(record, key));
    }
    push_member(&mut line, "exc", "null");
    line.push('}');
    line
}

/// Append `"key":value` to the object text `line`, after a comma when the
/// object has a member already. Pure.
fn push_member(line: &mut String, key: &str, value: &str) {
    if !line.ends_with('{') {
        line.push(',');
    }
    line.push_str(&json_text(key));
    line.push(':');
    line.push_str(value);
}

/// The time of a record, ISO 8601 in UTC with microseconds and the offset
/// `+00:00`, as Python's `datetime.isoformat` writes it. Pure.
fn ts_text(created: SystemTime) -> String {
    DateTime::<Utc>::from(created).to_rfc3339_opts(SecondsFormat::Micros, false)
}

/// The JSON value of the context key `key` of `record`. Pure.
fn context_value(record: &CoreLogRecord, key: &str) -> String {
    match record.fields.iter().find(|field| field.name == key) {
        None => "null".to_owned(),
        Some(field) => match &field.value {
            FieldValue::Text(text) => json_text(text),
            FieldValue::Signed(value) => value.to_string(),
            FieldValue::Unsigned(value) => value.to_string(),
            FieldValue::Bool(value) => value.to_string(),
            FieldValue::Float(value) => json_text(&value.to_string()),
        },
    }
}

/// `text` as an ASCII JSON string: the escapes of `serde_json`, and each
/// character outside ASCII as one `\u` escape per UTF-16 unit, in lowercase
/// hex. Pure.
fn json_text(text: &str) -> String {
    let escaped = serde_json::to_string(text).expect("a str always writes as a JSON string");
    match escaped.is_ascii() {
        true => escaped,
        false => {
            let mut ascii = String::with_capacity(escaped.len() + 16);
            for character in escaped.chars() {
                match character.is_ascii() {
                    true => ascii.push(character),
                    false => {
                        let mut units = [0_u16; 2];
                        for unit in character.encode_utf16(&mut units) {
                            write!(ascii, "\\u{unit:04x}")
                                .expect("a write to a String never fails");
                        }
                    }
                }
            }
            ascii
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;
    use crate::logging::bridge::RecordField;
    use crate::logging::names::LoggerName;

    /// The keys of a JSON line, in order: the keys of Python's
    /// `JsonLogsFormatter`.
    const JSON_LOG_KEYS: [&str; 11] = [
        "ts",
        "level",
        "logger",
        "message",
        "process",
        "task_id",
        "task_name",
        "attempt",
        "trace_id",
        "span_id",
        "exc",
    ];

    fn record(message: &str, fields: Vec<RecordField>) -> CoreLogRecord {
        let mut record = CoreLogRecord::bridge(RecordLevel::Info, message.to_owned());
        record.logger = LoggerName::Worker;
        record.created = UNIX_EPOCH + Duration::from_micros(1_795_608_000_123_456);
        record.fields = fields;
        record
    }

    fn keys_of(line: &str) -> Vec<String> {
        let value: serde_json::Value = serde_json::from_str(line).expect("the line is JSON");
        value
            .as_object()
            .expect("the line is one object")
            .keys()
            .cloned()
            .collect()
    }

    #[test]
    fn a_record_with_no_context_field_has_every_key_with_null_context_values() {
        let line = json_line(&record("worker ready", Vec::new()), 4242);
        assert_eq!(
            line,
            r#"{"ts":"2026-11-25T12:00:00.123456+00:00","level":"INFO","logger":"horsies.worker","message":"worker ready","process":4242,"task_id":null,"task_name":null,"attempt":null,"trace_id":null,"span_id":null,"exc":null}"#
        );
    }

    #[test]
    fn a_context_field_takes_its_key_and_another_field_has_no_key() {
        let fields = vec![
            RecordField {
                name: "task_id",
                value: FieldValue::Text("0b6e6d1c-6f53-4c5e-9d0a-1f1b2c3d4e5f".to_owned()),
            },
            RecordField {
                name: "attempt",
                value: FieldValue::Unsigned(2),
            },
            RecordField {
                name: "settlement",
                value: FieldValue::Text("held".to_owned()),
            },
        ];
        let line = json_line(&record("attempt end settled", fields), 1);
        let value: serde_json::Value = serde_json::from_str(&line).expect("the line is JSON");
        assert_eq!(value["task_id"], "0b6e6d1c-6f53-4c5e-9d0a-1f1b2c3d4e5f");
        assert_eq!(value["attempt"], 2);
        assert_eq!(keys_of(&line), JSON_LOG_KEYS.map(str::to_owned).to_vec());
    }

    #[test]
    fn a_flag_stays_a_flag_and_a_float_is_its_text() {
        let fields = vec![
            RecordField {
                name: "trace_id",
                value: FieldValue::Bool(false),
            },
            RecordField {
                name: "span_id",
                value: FieldValue::Float(1.5),
            },
            RecordField {
                name: "task_name",
                value: FieldValue::Signed(-3),
            },
        ];
        let value: serde_json::Value =
            serde_json::from_str(&json_line(&record("m", fields), 1)).expect("the line is JSON");
        assert_eq!(value["trace_id"], false);
        assert_eq!(value["span_id"], "1.5");
        assert_eq!(value["task_name"], -3);
    }

    #[test]
    fn the_line_is_ascii_with_lowercase_unicode_escapes_and_one_line() {
        let line = json_line(&record("caf\u{e9} \u{1f40e}\nnext", Vec::new()), 1);
        assert!(line.is_ascii(), "{line}");
        assert!(!line.contains('\n'), "{line}");
        assert!(
            line.contains(r#""message":"caf\u00e9 \ud83d\udc0e\nnext""#),
            "{line}"
        );
        let value: serde_json::Value = serde_json::from_str(&line).expect("the line is JSON");
        assert_eq!(value["message"], "caf\u{e9} \u{1f40e}\nnext");
    }

    #[test]
    fn each_format_writes_its_line_and_the_text_format_is_the_default() {
        assert_eq!(
            LogFormatKind::of_code(TEXT_LOGS_CODE),
            LogFormatKind::TextLogs
        );
        let logged = record("worker ready", Vec::new());
        assert_eq!(
            line_in(LogFormatKind::TextLogs, &logged),
            logged.stderr_line()
        );
        assert_eq!(
            keys_of(&line_in(LogFormatKind::JsonLogs, &logged)),
            JSON_LOG_KEYS.map(str::to_owned).to_vec()
        );
    }

    #[test]
    fn a_stored_format_reads_back() {
        for format in [LogFormatKind::JsonLogs, LogFormatKind::TextLogs] {
            assert_eq!(LogFormatKind::of_code(format.code()), format);
        }
    }
}

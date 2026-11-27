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

/// U+007F, the one ASCII character that Python's `json.dumps` escapes and
/// `serde_json` does not.
const DELETE: char = '\u{7f}';

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
/// example, why a batch goes to stderr) in `format`, with no newline:
/// the text as it is, or in the JSON format the line of a WARNING record of
/// the bridge logger with the text as its message. Pure but for the clock,
/// the thread name and the process id.
pub fn notice_line_in(format: LogFormatKind, text: String) -> String {
    match format {
        LogFormatKind::TextLogs => text,
        LogFormatKind::JsonLogs => json_line(
            &CoreLogRecord::bridge(RecordLevel::Warning, text),
            std::process::id(),
        ),
    }
}

/// The stderr text of a record that cannot enter the channel, with the drop
/// counts `counts`, in `format`, with no newline: in the text format one line
/// with the counts after the record, in the JSON format the line of the
/// record and then the notice line of the counts. Pure but for the clock,
/// the thread name and the process id.
pub fn spill_text(format: LogFormatKind, record: &CoreLogRecord, counts: String) -> String {
    match format {
        LogFormatKind::TextLogs => format!("{} ({counts})", record.stderr_line()),
        LogFormatKind::JsonLogs => format!(
            "{}\n{}",
            line_in(LogFormatKind::JsonLogs, record),
            notice_line_in(LogFormatKind::JsonLogs, counts)
        ),
    }
}

/// The JSON line of `record`, written by the process `process`, with no
/// newline: the keys `ts`, `level`, `logger`, `message`, `process`, then
/// the context keys ([`CONTEXT_KEYS`]), then `exc`. A context key holds the
/// value of the first record field of its name: a text as a JSON string, an
/// integer as a JSON number, a flag as a JSON flag, and a float as the
/// string of Python's `str` of the float ([`python_float_text`]). `exc` is
/// always `null`: a core record carries no exception. Pure.
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
            FieldValue::Float(value) => json_text(&python_float_text(*value)),
        },
    }
}

/// The exponent of a decimal point at or below which, and the one above
/// which, Python's `repr` of a float takes the exponent form
/// (`float_repr_style` `short`).
const PYTHON_EXPONENT_FORM_LOW: i32 = -4;
const PYTHON_EXPONENT_FORM_HIGH: i32 = 16;

/// The text of `value` as Python's `str` writes it: the shortest digits that
/// read back as `value`; the exponent form (`1e+16`, `2.5e-07`: a sign and at
/// least two exponent digits) when the decimal point is at or below
/// [`PYTHON_EXPONENT_FORM_LOW`] or above [`PYTHON_EXPONENT_FORM_HIGH`]; else
/// the fixed form with at least one fraction digit (`1.0`, `-0.0`). `nan`,
/// `inf` and `-inf` for the values that are no number. Pure.
fn python_float_text(value: f64) -> String {
    match (
        value.is_nan(),
        value.is_infinite(),
        value.is_sign_negative(),
    ) {
        (true, _, _) => "nan".to_owned(),
        (false, true, true) => "-inf".to_owned(),
        (false, true, false) => "inf".to_owned(),
        (false, false, _) => finite_float_text(value),
    }
}

/// The text of a finite `value` ([`python_float_text`]). Pure.
fn finite_float_text(value: f64) -> String {
    let shortest = format!("{value:e}");
    let (mantissa, exponent) = shortest
        .split_once('e')
        .expect("the exponent form of a finite float has an exponent");
    let exponent: i32 = exponent
        .parse()
        .expect("the exponent of a finite float is a small integer");
    let (sign, mantissa) = match mantissa.strip_prefix('-') {
        Some(unsigned) => ("-", unsigned),
        None => ("", mantissa),
    };
    let point = exponent + 1;
    match point <= PYTHON_EXPONENT_FORM_LOW || point > PYTHON_EXPONENT_FORM_HIGH {
        true => {
            let exponent_sign = match exponent < 0 {
                true => '-',
                false => '+',
            };
            format!(
                "{sign}{mantissa}e{exponent_sign}{:02}",
                exponent.unsigned_abs()
            )
        }
        false => format!("{sign}{}", fixed_digits(&mantissa.replace('.', ""), point)),
    }
}

/// The digits `digits` with the decimal point after `point` digits, and at
/// least one digit on each side of the point. Pure.
fn fixed_digits(digits: &str, point: i32) -> String {
    let count = i32::try_from(digits.len()).expect("a float has at most 17 digits");
    match (point <= 0, point >= count) {
        (true, _) => format!("0.{}{digits}", "0".repeat(point.unsigned_abs() as usize)),
        (false, true) => format!(
            "{digits}{}.0",
            "0".repeat((point - count).unsigned_abs() as usize)
        ),
        (false, false) => {
            let (whole, fraction) = digits.split_at(point.unsigned_abs() as usize);
            format!("{whole}.{fraction}")
        }
    }
}

/// `text` as an ASCII JSON string, as Python's `json.dumps` writes it: the
/// escapes of `serde_json` (the short escapes and `\u00xx` below U+0020), and
/// U+007F and each character outside ASCII as one `\u` escape per UTF-16
/// unit, in lowercase hex. Pure.
fn json_text(text: &str) -> String {
    let escaped = serde_json::to_string(text).expect("a str always writes as a JSON string");
    match escaped
        .chars()
        .all(|character| character.is_ascii() && character != DELETE)
    {
        true => escaped,
        false => {
            let mut ascii = String::with_capacity(escaped.len() + 16);
            for character in escaped.chars() {
                match character.is_ascii() && character != DELETE {
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

    /// Python's `str` of each float, from `json.dumps(str(value))` of
    /// CPython 3.
    #[test]
    fn a_float_is_the_text_of_python_str() {
        let vectors: [(f64, &str); 18] = [
            (1.0, "1.0"),
            (1.5, "1.5"),
            (-0.0, "-0.0"),
            (0.1, "0.1"),
            (100.0, "100.0"),
            (1e15, "1000000000000000.0"),
            (1e16, "1e+16"),
            (1.5e16, "1.5e+16"),
            (1e-4, "0.0001"),
            (1e-5, "1e-05"),
            (123_456_789.123, "123456789.123"),
            (5e-324, "5e-324"),
            (f64::MAX, "1.7976931348623157e+308"),
            (f64::NAN, "nan"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (2.5e-7, "2.5e-07"),
            (12_345_678_901_234_567.0, "1.2345678901234568e+16"),
        ];
        for (value, text) in vectors {
            assert_eq!(python_float_text(value), text, "{value:e}");
        }
    }

    /// The escapes of `json.dumps` of CPython 3 with `ensure_ascii`.
    #[test]
    fn a_text_has_the_escapes_of_python_json_dumps() {
        assert_eq!(
            json_text("a\u{7f}b\u{1f}\u{0}\u{8}\u{c}\n\r\t\"\\/\u{e9}\u{1f40e}\u{2028}"),
            r#""a\u007fb\u001f\u0000\b\f\n\r\t\"\\/\u00e9\ud83d\udc0e\u2028""#
        );
    }

    #[test]
    fn a_json_notice_is_a_warning_record_of_the_bridge_and_a_text_notice_is_the_text() {
        let text = "horsies: 3 log records go to stderr: the JavaScript sink threw".to_owned();
        assert_eq!(notice_line_in(LogFormatKind::TextLogs, text.clone()), text);
        let value: serde_json::Value =
            serde_json::from_str(&notice_line_in(LogFormatKind::JsonLogs, text.clone()))
                .expect("the notice is JSON");
        assert_eq!(value["level"], "WARNING");
        assert_eq!(value["logger"], "horsies.bridge");
        assert_eq!(value["message"], text.as_str());
        assert_eq!(
            value
                .as_object()
                .map(|object| object.keys().cloned().collect::<Vec<_>>()),
            Some(JSON_LOG_KEYS.map(str::to_owned).to_vec())
        );
    }

    #[test]
    fn a_json_spill_is_the_record_line_then_the_notice_line_of_the_counts() {
        let logged = record("worker ready", Vec::new());
        let counts = "horsies log channel full or closed; dropped TRACE/DEBUG/INFO records: 2, \
                      WARNING/ERROR records written to stderr: 1"
            .to_owned();
        let text = spill_text(LogFormatKind::JsonLogs, &logged, counts.clone());
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).expect("each spill line is JSON"))
            .collect();
        assert_eq!(lines.len(), 2, "{text}");
        assert_eq!(lines[0]["message"], "worker ready");
        assert_eq!(lines[0]["logger"], "horsies.worker");
        assert_eq!(lines[1]["level"], "WARNING");
        assert_eq!(lines[1]["logger"], "horsies.bridge");
        assert_eq!(lines[1]["message"], counts.as_str());
        assert_eq!(
            spill_text(LogFormatKind::TextLogs, &logged, counts.clone()),
            format!("{} ({counts})", logged.stderr_line())
        );
    }

    #[test]
    fn a_stored_format_reads_back() {
        for format in [LogFormatKind::JsonLogs, LogFormatKind::TextLogs] {
            assert_eq!(LogFormatKind::of_code(format.code()), format);
        }
    }
}

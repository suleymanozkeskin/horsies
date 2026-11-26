//! The core's task error of a body, built from its parts.
//!
//! - The code: a user code that `UserCode::parse` accepts (a built-in or a
//!   reserved name is refused, as Python and Rust refuse it when the error
//!   is built), or a library code.
//! - The data: none, the codec's stored `data` text, or data that the codec
//!   cannot write. The bridge stores data that the codec cannot write as
//!   `{"data_refused": "<cause>"}`, with the cause that Python stores, and
//!   logs it at ERROR on `horsies.codec`; the code and the message are kept,
//!   as the Rust client's `TaskError::with_data` keeps them. The core applies
//!   its own storage rule when it writes the error: data with a key that only
//!   the engine writes, or deeper than its bound, is stored in the same form.
//! - Starts no core. It starts the log bridge of the process (the forwarder
//!   thread), so the ERROR record reaches the log sink.

use horsies_core::core::codec::{UserValueRefused, STRICT_JSON_ERROR_CAUSE};
use horsies_core::core::task::error::DATA_REFUSED_KEY;
use horsies_core::core::task::ExceptionRecord;
use horsies_core::{BuiltInTaskCode, JsonText, TaskError, TaskErrorCode, TaskErrorKind, UserCode};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::logging::bridge::{started_log, LogBridge};
use crate::logging::names::LoggerName;
use crate::process_core::runtime::{CallRefusal, StartFailure};
use crate::sends::send::OptionalText;
use crate::values::boundary::BoundaryRefusal;

/// The logger of the record of data that the codec cannot write, as Python
/// logs it (`reportable_error`, `horsies.codec`).
const CODEC_TARGET: &str = LoggerName::Codec.dotted_name();

/// The code of an error that a body gives.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorCodePart {
    /// A code that the task chose.
    User { code: String },
    /// A built-in code, as its text (`UNHANDLED_EXCEPTION`).
    Library { code: String },
}

/// The exception record of a body error.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExceptionPart {
    NoException,
    /// The record of a thrown value: the class name (`error.name`), the
    /// module, the message, the repr and the stack.
    Record {
        class_name: String,
        module: String,
        message: String,
        repr: String,
        traceback: String,
    },
}

/// Why the codec cannot write the `data` of an error.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataRefusedReason {
    /// A value that is not JSON-native, such as a non-finite number.
    NotJsonNative,
    /// An integer longer than the integer text limit of the stored form.
    IntegerTooLong,
}

impl DataRefusedReason {
    /// The cause that Python stores in `data_refused` for the reason: the
    /// core's text of each. Pure.
    fn stored_cause(self) -> &'static str {
        match self {
            Self::NotJsonNative => STRICT_JSON_ERROR_CAUSE,
            Self::IntegerTooLong => UserValueRefused::IntegerTooLong.stored_cause(),
        }
    }
}

/// The `data` of an error that a body gives.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorDataPart {
    /// The error has no data.
    NoData,
    /// The codec's stored `data` text.
    Text { text: String },
    /// The codec cannot write the data, for this reason.
    NotStorable { reason: DataRefusedReason },
}

/// The parts of the error of a body.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskErrorParts {
    pub code: ErrorCodePart,
    pub message: OptionalText,
    pub data: ErrorDataPart,
    pub exception: ExceptionPart,
}

/// The core's task error of a body. JavaScript gets it from
/// `buildTaskError`; it has no constructor.
#[napi]
#[derive(Clone, Debug)]
pub struct TaskErrorRecord {
    error: TaskError,
}

impl TaskErrorRecord {
    /// The core's error, for the body answer of a child area.
    #[allow(
        dead_code,
        reason = "the Node child area gives the error of a body to the child host"
    )]
    pub fn error(&self) -> &TaskError {
        &self.error
    }
}

#[napi]
impl TaskErrorRecord {
    /// The stored error object text, as the core writes it into a result
    /// envelope (the core's storage rule of `data` applied). Pure.
    #[napi(ts_return_type = "NativeEnvelope<string>")]
    pub fn stored_text(&self) -> Envelope<String> {
        sync_call("task_error_stored_text", || {
            match JsonText::of(&self.error) {
                Ok(text) => Envelope::Value(text.get().to_owned()),
                Err(refused) => Envelope::Thrown(NativeThrown::ClientDefect {
                    subject: "storedText".to_owned(),
                    rule: format!("a task error writes as strict JSON: {refused}"),
                }),
            }
        })
    }
}

/// The outcome of a task error build.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Debug)]
pub enum TaskErrorBuild {
    Built {
        error: TaskErrorRecord,
    },
    /// The core refuses the user code: the cause names the rule.
    Refused {
        cause: String,
    },
}

/// Build the core's task error of a body from its parts. A library code that
/// is not a built-in code, or a data text that is not strict JSON, throws
/// `CLIENT_DEFECT`: the package gives only the codes of its catalog and the
/// texts of its codec. Starts no core; starts the log bridge of the process,
/// and a forwarder thread that cannot start throws `CORE_START_FAILED`.
#[napi(ts_return_type = "NativeEnvelope<TaskErrorBuild>")]
pub fn build_task_error(parts: TaskErrorParts) -> Envelope<TaskErrorBuild> {
    sync_call("build_task_error", || match started_log() {
        Err(failure) => Envelope::Thrown(
            CallRefusal::StartFailed(StartFailure::LogForwarderStartFailed {
                cause: failure.cause,
            })
            .thrown(),
        ),
        Ok(log) => match task_error_build(parts, log) {
            Ok(build) => Envelope::Value(build),
            Err(refusal) => Envelope::Thrown(refusal.into()),
        },
    })
}

/// The build of a task error, or the refusal of a part that breaks a
/// boundary rule. The record of data that the codec cannot write goes to
/// `log`. Pure apart from the log.
fn task_error_build(
    parts: TaskErrorParts,
    log: &LogBridge,
) -> Result<TaskErrorBuild, BoundaryRefusal> {
    let error_code = match parts.code {
        ErrorCodePart::User { code } => match UserCode::parse(code) {
            Ok(user) => TaskErrorCode::User(user),
            Err(refused) => {
                return Ok(TaskErrorBuild::Refused {
                    cause: refused.to_string(),
                })
            }
        },
        ErrorCodePart::Library { code } => {
            TaskErrorCode::BuiltIn(BuiltInTaskCode::parse(&code).ok_or_else(|| {
                BoundaryRefusal::new("code.code", "the text of a built-in task code")
            })?)
        }
    };
    let data = match parts.data {
        ErrorDataPart::NoData => None,
        ErrorDataPart::Text { text } => Some(JsonText::parse(&text).map_err(|_| {
            BoundaryRefusal::new("data.text", "strict JSON text, as the codec writes it")
        })?),
        ErrorDataPart::NotStorable { reason } => Some(refused_data(log, &error_code, reason)),
    };
    Ok(TaskErrorBuild::Built {
        error: TaskErrorRecord {
            error: TaskError {
                exception: exception_of(parts.exception),
                error_code,
                data,
                message: parts.message.text(),
                kind: TaskErrorKind::Plain,
            },
        },
    })
}

/// `{"data_refused": "<cause>"}` for data that the codec cannot write, and
/// its ERROR record on `log`, with the code of the error. Pure apart from
/// the log.
fn refused_data(log: &LogBridge, code: &TaskErrorCode, reason: DataRefusedReason) -> JsonText {
    let cause = reason.stored_cause();
    tracing::dispatcher::with_default(log.dispatch(), || {
        tracing::error!(
            target: CODEC_TARGET,
            "data of a TaskError with code {code} cannot cross the wire: {cause}"
        );
    });
    let mut object = serde_json::Map::new();
    let _previous = object.insert(
        DATA_REFUSED_KEY.to_owned(),
        serde_json::Value::String(cause.to_owned()),
    );
    JsonText::of(&serde_json::Value::Object(object))
        .expect("an object of one text member writes as strict JSON")
}

/// The core's exception record of a part. Pure.
fn exception_of(part: ExceptionPart) -> Option<Box<ExceptionRecord>> {
    match part {
        ExceptionPart::NoException => None,
        ExceptionPart::Record {
            class_name,
            module,
            message,
            repr,
            traceback,
        } => Some(Box::new(ExceptionRecord {
            type_name: class_name,
            module,
            message,
            repr,
            traceback,
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::levels::RecordLevel;

    /// A log bridge of a test that reads none of its records.
    fn quiet_log() -> LogBridge {
        let (log, _records) = LogBridge::new();
        log
    }

    fn user_code(code: &str) -> ErrorCodePart {
        ErrorCodePart::User {
            code: code.to_owned(),
        }
    }

    fn parts(code: ErrorCodePart, data: ErrorDataPart) -> TaskErrorParts {
        TaskErrorParts {
            code,
            message: OptionalText::Text {
                text: "card declined".to_owned(),
            },
            data,
            exception: ExceptionPart::NoException,
        }
    }

    fn data_text(text: &str) -> ErrorDataPart {
        ErrorDataPart::Text {
            text: text.to_owned(),
        }
    }

    fn stored(build: TaskErrorBuild) -> String {
        match build {
            TaskErrorBuild::Built { error } => match error.stored_text() {
                Envelope::Value(text) => text,
                Envelope::Thrown(thrown) => panic!("the error writes, got {thrown:?}"),
            },
            TaskErrorBuild::Refused { cause } => panic!("the error is built, got {cause}"),
        }
    }

    #[test]
    fn a_user_code_with_data_is_stored_with_its_four_keys() {
        let build = task_error_build(
            parts(user_code("CARD_DECLINED"), data_text(r#"{"attempt":2}"#)),
            &quiet_log(),
        )
        .expect("the parts cross");
        assert_eq!(
            stored(build),
            r#"{"exception":null,"error_code":"CARD_DECLINED","data":{"attempt":2},"message":"card declined"}"#
        );
    }

    #[test]
    fn an_error_with_no_data_stores_null_data() {
        let build = task_error_build(
            parts(user_code("CARD_DECLINED"), ErrorDataPart::NoData),
            &quiet_log(),
        )
        .expect("the parts cross");
        assert!(stored(build).contains(r#""data":null"#));
    }

    #[test]
    fn a_built_in_name_as_a_user_code_is_refused_with_the_cause() {
        let build = task_error_build(
            parts(user_code("TASK_TIMEOUT"), ErrorDataPart::NoData),
            &quiet_log(),
        )
        .expect("the parts cross");
        assert!(
            matches!(build, TaskErrorBuild::Refused { cause } if cause.contains("TASK_TIMEOUT"))
        );
    }

    /// `WORKFLOW_STOPPED` is a reserved name: only the engine writes it.
    #[test]
    fn a_reserved_name_as_a_user_code_is_refused_with_the_cause() {
        let build = task_error_build(
            parts(user_code("WORKFLOW_STOPPED"), ErrorDataPart::NoData),
            &quiet_log(),
        )
        .expect("the parts cross");
        assert!(
            matches!(build, TaskErrorBuild::Refused { cause } if cause.contains("WORKFLOW_STOPPED"))
        );
    }

    #[test]
    fn a_library_code_with_an_exception_record_is_stored_with_its_tag() {
        let build = task_error_build(
            TaskErrorParts {
                exception: ExceptionPart::Record {
                    class_name: "TypeError".to_owned(),
                    module: "builtins".to_owned(),
                    message: "x is not a function".to_owned(),
                    repr: "TypeError: x is not a function".to_owned(),
                    traceback: "TypeError: x is not a function\n    at body".to_owned(),
                },
                ..parts(
                    ErrorCodePart::Library {
                        code: "UNHANDLED_EXCEPTION".to_owned(),
                    },
                    ErrorDataPart::NoData,
                )
            },
            &quiet_log(),
        )
        .expect("the parts cross");
        let text = stored(build);
        assert!(
            text.contains(r#""error_code":{"__builtin_task_code__":"UNHANDLED_EXCEPTION"}"#),
            "{text}"
        );
        assert!(text.contains(r#""type":"TypeError""#), "{text}");
    }

    #[test]
    fn data_with_an_engine_key_is_replaced_by_the_core_and_the_code_stays() {
        let build = task_error_build(
            parts(user_code("CARD_DECLINED"), data_text(r#"{"__h_secret":1}"#)),
            &quiet_log(),
        )
        .expect("the parts cross");
        let text = stored(build);
        assert!(text.contains(r#""data":{"data_refused":"#), "{text}");
        assert!(text.contains(r#""error_code":"CARD_DECLINED""#), "{text}");
    }

    /// Data that the codec cannot write is stored with Python's cause, the
    /// code and the message are kept, and the replacement is an ERROR record
    /// of `horsies.codec` that names the code and the cause.
    #[test]
    fn data_that_is_not_json_native_is_stored_as_data_refused_and_logged_at_error() {
        let (log, records) = LogBridge::new();
        let build = task_error_build(
            parts(
                user_code("CARD_DECLINED"),
                ErrorDataPart::NotStorable {
                    reason: DataRefusedReason::NotJsonNative,
                },
            ),
            &log,
        )
        .expect("the parts cross");
        assert_eq!(
            stored(build),
            r#"{"exception":null,"error_code":"CARD_DECLINED","data":{"data_refused":"StrictJsonError"},"message":"card declined"}"#
        );
        let record = records
            .try_recv()
            .expect("the replacement reached the channel");
        assert_eq!(record.logger, LoggerName::Codec);
        assert_eq!(record.level, RecordLevel::Error);
        assert_eq!(
            record.message,
            "data of a TaskError with code CARD_DECLINED cannot cross the wire: StrictJsonError"
        );
    }

    #[test]
    fn an_integer_that_is_too_long_is_stored_with_the_serialization_error_cause() {
        let build = task_error_build(
            parts(
                user_code("CARD_DECLINED"),
                ErrorDataPart::NotStorable {
                    reason: DataRefusedReason::IntegerTooLong,
                },
            ),
            &quiet_log(),
        )
        .expect("the parts cross");
        assert!(
            stored(build).contains(r#""data":{"data_refused":"SerializationError"}"#),
            "the cause of an integer that is too long"
        );
    }

    #[test]
    fn an_unknown_library_code_and_a_data_text_that_is_not_json_are_boundary_refusals() {
        let unknown = task_error_build(
            parts(
                ErrorCodePart::Library {
                    code: "NOT_A_CODE".to_owned(),
                },
                ErrorDataPart::NoData,
            ),
            &quiet_log(),
        );
        assert!(matches!(unknown, Err(refusal) if refusal.subject == "code.code"));
        let not_json = task_error_build(
            parts(user_code("CARD_DECLINED"), data_text("{not json")),
            &quiet_log(),
        );
        assert!(matches!(not_json, Err(refusal) if refusal.subject == "data.text"));
    }
}

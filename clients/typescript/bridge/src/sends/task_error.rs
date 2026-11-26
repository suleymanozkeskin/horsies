//! The core's task error of a body, built from its parts.
//!
//! - The code: a user code that `UserCode::parse` accepts (a built-in or a
//!   reserved name is refused, as Python and Rust refuse it when the error
//!   is built), or a library code.
//! - The data: the codec's stored `data` text. The core applies its own
//!   storage rule when it writes the error: data with a key that only the
//!   engine writes, or deeper than its bound, is stored as
//!   `{"data_refused": "<cause>"}`, with the code and the message kept.
//! - Pure: starts no core.

use horsies_core::core::task::ExceptionRecord;
use horsies_core::{BuiltInTaskCode, JsonText, TaskError, TaskErrorCode, TaskErrorKind, UserCode};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::sends::send::OptionalText;
use crate::values::boundary::BoundaryRefusal;

/// The `data` text of an error with no data.
const NO_DATA_TEXT: &str = "null";

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

/// The parts of the error of a body.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskErrorParts {
    pub code: ErrorCodePart,
    pub message: OptionalText,
    /// The codec's stored `data` text; `null` for no data.
    pub data_json: String,
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
        sync_call("stored_text", || match JsonText::of(&self.error) {
            Ok(text) => Envelope::Value(text.get().to_owned()),
            Err(refused) => Envelope::Thrown(NativeThrown::ClientDefect {
                subject: "storedText".to_owned(),
                rule: format!("a task error writes as strict JSON: {refused}"),
            }),
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
/// texts of its codec. Starts nothing.
#[napi(ts_return_type = "NativeEnvelope<TaskErrorBuild>")]
pub fn build_task_error(parts: TaskErrorParts) -> Envelope<TaskErrorBuild> {
    sync_call("build_task_error", || match task_error_build(parts) {
        Ok(build) => Envelope::Value(build),
        Err(refusal) => Envelope::Thrown(refusal.into()),
    })
}

/// The build of a task error, or the refusal of a part that breaks a
/// boundary rule. Pure.
fn task_error_build(parts: TaskErrorParts) -> Result<TaskErrorBuild, BoundaryRefusal> {
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
    let data = match parts.data_json.as_str() {
        NO_DATA_TEXT => None,
        text => Some(JsonText::parse(text).map_err(|_| {
            BoundaryRefusal::new("dataJson", "strict JSON text, as the codec writes it")
        })?),
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

    fn parts(code: ErrorCodePart, data_json: &str) -> TaskErrorParts {
        TaskErrorParts {
            code,
            message: OptionalText::Text {
                text: "card declined".to_owned(),
            },
            data_json: data_json.to_owned(),
            exception: ExceptionPart::NoException,
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
        let build = task_error_build(parts(
            ErrorCodePart::User {
                code: "CARD_DECLINED".to_owned(),
            },
            r#"{"attempt":2}"#,
        ))
        .expect("the parts cross");
        assert_eq!(
            stored(build),
            r#"{"exception":null,"error_code":"CARD_DECLINED","data":{"attempt":2},"message":"card declined"}"#
        );
    }

    #[test]
    fn a_built_in_name_as_a_user_code_is_refused_with_the_cause() {
        let build = task_error_build(parts(
            ErrorCodePart::User {
                code: "TASK_TIMEOUT".to_owned(),
            },
            NO_DATA_TEXT,
        ))
        .expect("the parts cross");
        assert!(
            matches!(build, TaskErrorBuild::Refused { cause } if cause.contains("TASK_TIMEOUT"))
        );
    }

    #[test]
    fn a_library_code_with_an_exception_record_is_stored_with_its_tag() {
        let build = task_error_build(TaskErrorParts {
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
                NO_DATA_TEXT,
            )
        })
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
        let build = task_error_build(parts(
            ErrorCodePart::User {
                code: "CARD_DECLINED".to_owned(),
            },
            r#"{"__h_secret":1}"#,
        ))
        .expect("the parts cross");
        let text = stored(build);
        assert!(text.contains(r#""data":{"data_refused":"#), "{text}");
        assert!(text.contains(r#""error_code":"CARD_DECLINED""#), "{text}");
    }

    #[test]
    fn an_unknown_library_code_and_a_data_text_that_is_not_json_are_boundary_refusals() {
        let unknown = task_error_build(parts(
            ErrorCodePart::Library {
                code: "NOT_A_CODE".to_owned(),
            },
            NO_DATA_TEXT,
        ));
        assert!(matches!(unknown, Err(refusal) if refusal.subject == "code.code"));
        let not_json = task_error_build(parts(
            ErrorCodePart::User {
                code: "CARD_DECLINED".to_owned(),
            },
            "{not json",
        ));
        assert!(matches!(not_json, Err(refusal) if refusal.subject == "dataJson"));
    }
}

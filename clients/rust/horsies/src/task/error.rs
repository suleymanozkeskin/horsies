//! The error of a task: `TaskError<E>`, with the library codes as the core's
//! closed catalog and the task's own codes as the user's enum `E`.
//!
//! The stored form is the core's `TaskError` (Python `encode_task_error`):
//! `exception`, `error_code`, `data`, `message`, and the two fields of a
//! sub-workflow failure. This module converts in both directions.

use horsies_core::core::codec::{check_user_value_text, StrictJsonError, UserValueRefused};
use horsies_core::core::task::error::{
    ExceptionRecord, TaskErrorKind, DATA_REFUSED_KEY, ERROR_DATA_DEPTH_LIMIT,
};
use horsies_core::{BuiltInTaskCode, JsonText, JsonTextRefused, SubWorkflowFailure};

use super::user_code::{stored_user_code, UserErrorCode};

/// The cause that Python stores in `data_refused` for a value that is not
/// JSON-native: a non-finite float, a map key that is not a string, or a
/// value that does not serialize (`_validate_json_native` raises
/// `StrictJsonError`).
const NOT_JSON_NATIVE_CAUSE: &str = "StrictJsonError";

/// The code of a task error.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TaskErrorCode<E> {
    /// A code of the library's catalog (Python `BuiltInTaskCode`): the
    /// library records it for a timeout, a crash, a cancel, a panic, a
    /// value that does not serialize, and other ends that it decides.
    Library(BuiltInTaskCode),
    /// A code of the task's own enum.
    User(E),
}

impl<E: UserErrorCode> TaskErrorCode<E> {
    /// The stored code text.
    pub fn text(&self) -> String {
        match self {
            Self::Library(code) => code.value(),
            Self::User(code) => code.code().to_owned(),
        }
    }
}

impl<E: UserErrorCode> std::fmt::Display for TaskErrorCode<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Library(code) => write!(formatter, "{code}"),
            Self::User(code) => formatter.write_str(code.code()),
        }
    }
}

/// The error of a task (Python `TaskError`).
///
/// A task body returns one with [`TaskError::user`]; the library records the
/// others. `data` is set with [`TaskError::with_data`], which never fails.
/// The exception record and the sub-workflow failure are set only by the
/// library.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskError<E> {
    code: TaskErrorCode<E>,
    message: Option<String>,
    data: Option<JsonText>,
    exception: Option<Box<ExceptionRecord>>,
    kind: TaskErrorKind,
}

/// A stored error whose user code the code type `E` does not name: the task
/// that stored it has another code type, or a version of the code enum
/// without this code.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("stored user error code {code:?} is not a code of {code_type}")]
pub struct UnknownUserCode {
    /// The stored code text.
    pub code: String,
    /// The code type that the read asked for.
    pub code_type: &'static str,
}

impl<E: UserErrorCode> TaskError<E> {
    /// An error with a code of the task's own enum.
    pub fn user(code: E, message: impl Into<String>) -> Self {
        Self::with_code(TaskErrorCode::User(code), message.into())
    }

    /// An error with a library code. The library builds these for the ends
    /// it decides; a task body may return one too, as in Python.
    pub fn library(code: impl Into<BuiltInTaskCode>, message: impl Into<String>) -> Self {
        Self::with_code(TaskErrorCode::Library(code.into()), message.into())
    }

    fn with_code(code: TaskErrorCode<E>, message: String) -> Self {
        Self {
            code,
            message: Some(message),
            data: None,
            exception: None,
            kind: TaskErrorKind::Plain,
        }
    }

    /// This error with `data`. Never fails: a value that cannot be stored is
    /// replaced with `{"data_refused": "<cause>"}`, and the code and the
    /// message are kept, so retry matching and outcome routing still work.
    /// A replacement is logged at ERROR with the code and the cause.
    ///
    /// A value cannot be stored when it is not JSON-native (a non-finite
    /// float, a map key that is not a string, a `Serialize` impl that fails),
    /// holds a key that only the engine writes (`__h_...`, `__horsies_...`,
    /// `__builtin_task_code__`), or is nested deeper than the error bound.
    pub fn with_data(mut self, data: impl serde::Serialize) -> Self {
        let stored = match error_data(&data) {
            Ok(value) => value,
            Err(cause) => {
                tracing::error!(
                    code = %self.code,
                    cause,
                    "the data of a TaskError cannot be stored; data_refused is stored in its place"
                );
                data_refused(cause)
            }
        };
        self.data = Some(stored);
        self
    }

    /// The code.
    pub fn code(&self) -> &TaskErrorCode<E> {
        &self.code
    }

    /// The message.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// The data, as its stored text: `get` gives the text, `decode::<T>`
    /// the value. An integer outside the `i64` and `u64` range keeps every
    /// digit, so it decodes as `u128` or `i128`.
    pub fn data(&self) -> Option<&JsonText> {
        self.data.as_ref()
    }

    /// The exception record: set by the library for a panic of the task body.
    pub fn exception(&self) -> Option<&ExceptionRecord> {
        self.exception.as_deref()
    }

    /// The failed child workflow, when this error is a sub-workflow failure.
    pub fn sub_workflow(&self) -> Option<&SubWorkflowFailure> {
        match &self.kind {
            TaskErrorKind::Plain => None,
            TaskErrorKind::SubWorkflow(failure) => Some(failure),
        }
    }

    /// The stored form. A user code becomes the core's `UserCode`.
    pub(crate) fn into_stored(self) -> horsies_core::TaskError {
        let error_code = match &self.code {
            TaskErrorCode::Library(code) => horsies_core::TaskErrorCode::BuiltIn(code.clone()),
            TaskErrorCode::User(code) => horsies_core::TaskErrorCode::User(stored_user_code(code)),
        };
        horsies_core::TaskError {
            exception: self.exception,
            error_code,
            data: self.data,
            message: self.message,
            kind: self.kind,
        }
    }

    /// The error of a stored error. Refuses a user code that `E` does not
    /// name.
    pub(crate) fn from_stored(stored: horsies_core::TaskError) -> Result<Self, UnknownUserCode> {
        let code = match stored.error_code {
            horsies_core::TaskErrorCode::BuiltIn(code) => TaskErrorCode::Library(code),
            horsies_core::TaskErrorCode::User(code) => match E::from_code(code.as_str()) {
                Some(user) => TaskErrorCode::User(user),
                None => {
                    return Err(UnknownUserCode {
                        code: code.as_str().to_owned(),
                        code_type: std::any::type_name::<E>(),
                    })
                }
            },
        };
        Ok(Self {
            code,
            message: stored.message,
            data: stored.data,
            exception: stored.exception,
            kind: stored.kind,
        })
    }
}

impl<E: UserErrorCode> std::fmt::Display for TaskError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.message {
            Some(message) => write!(formatter, "[{}] {message}", self.code),
            None => write!(formatter, "[{}]", self.code),
        }
    }
}

impl<E: UserErrorCode> std::error::Error for TaskError<E> {}

/// The stored text of `data`, or the cause that Python stores in
/// `data_refused`. The text is written by the core's strict writer, so an
/// integer of any width keeps every digit. Pure.
fn error_data(data: &impl serde::Serialize) -> Result<JsonText, &'static str> {
    let text = JsonText::of(data).map_err(|refused| written_data_cause(&refused))?;
    check_user_value_text(&text, ERROR_DATA_DEPTH_LIMIT)
        .map_err(|refused| refused.stored_cause())?;
    Ok(text)
}

/// The cause that Python stores for a value that does not write as strict
/// JSON: a value that is not JSON-native, an integer longer than Python's
/// limit, or a value deeper than any stored text. Pure.
fn written_data_cause(refused: &JsonTextRefused) -> &'static str {
    match refused {
        JsonTextRefused::NotJsonNative(_) => NOT_JSON_NATIVE_CAUSE,
        JsonTextRefused::NotStrictJson(StrictJsonError::IntegerTooLong { .. }) => {
            UserValueRefused::IntegerTooLong.stored_cause()
        }
        JsonTextRefused::NotStrictJson(_) => UserValueRefused::DeeperThanTextBound.stored_cause(),
    }
}

/// `{"data_refused": "<cause>"}`. Pure.
fn data_refused(cause: &'static str) -> JsonText {
    let mut object = serde_json::Map::new();
    let _previous = object.insert(
        DATA_REFUSED_KEY.to_owned(),
        serde_json::Value::String(cause.to_owned()),
    );
    JsonText::of(&serde_json::Value::Object(object))
        .expect("an object of one text member writes as strict JSON")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use horsies_core::{OperationalErrorCode, OutcomeCode, SubWorkflowSummary, UserCode};
    use serde_json::json;

    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    enum ChargeCode {
        CardDeclined,
    }

    impl UserErrorCode for ChargeCode {
        const CODES: &'static [&'static str] = &["CARD_DECLINED"];

        fn code(&self) -> &'static str {
            match self {
                Self::CardDeclined => "CARD_DECLINED",
            }
        }

        fn from_code(text: &str) -> Option<Self> {
            match text {
                "CARD_DECLINED" => Some(Self::CardDeclined),
                _ => None,
            }
        }
    }

    #[derive(serde::Serialize)]
    struct Ratio {
        ratio: f64,
    }

    fn declined() -> TaskError<ChargeCode> {
        TaskError::user(ChargeCode::CardDeclined, "card declined")
    }

    fn nested(depth: usize) -> serde_json::Value {
        (1..depth).fold(json!([1]), |inner, _| json!([inner]))
    }

    /// The data of `error` decoded as a JSON value.
    fn data_of(error: &TaskError<ChargeCode>) -> Option<serde_json::Value> {
        error.data().map(|data| data.decode().unwrap())
    }

    #[test]
    fn a_user_error_has_its_code_and_message() {
        let error = declined();
        assert_eq!(error.code(), &TaskErrorCode::User(ChargeCode::CardDeclined));
        assert_eq!(error.message(), Some("card declined"));
        assert_eq!(error.data(), None);
        assert_eq!(error.exception(), None);
        assert_eq!(error.sub_workflow(), None);
        assert_eq!(error.to_string(), "[CARD_DECLINED] card declined");
    }

    #[test]
    fn a_library_error_has_the_built_in_code() {
        let error = TaskError::<ChargeCode>::library(OutcomeCode::TaskTimeout, "too slow");
        assert_eq!(error.code().text(), "TASK_TIMEOUT");
    }

    #[test]
    fn finite_data_is_stored_as_it_is() {
        let error = declined().with_data(BTreeMap::from([("attempt", 2)]));
        assert_eq!(data_of(&error), Some(json!({"attempt": 2})));
        let value = json!({"field": "email"});
        assert_eq!(data_of(&declined().with_data(&value)), Some(value));
    }

    /// An integer outside the `i64` and `u64` range is stored with every
    /// digit and decodes as its type, as Python stores it.
    #[test]
    fn data_outside_i64_and_u64_keeps_every_digit() {
        let error = declined().with_data(BTreeMap::from([
            ("wide", i128::from(u64::MAX) + 1),
            ("below", i128::from(i64::MIN) - 1),
        ]));
        assert_eq!(
            error.data().map(JsonText::get),
            Some(r#"{"below":-9223372036854775809,"wide":18446744073709551616}"#)
        );
        let max = declined().with_data(u128::MAX);
        assert_eq!(
            max.data().map(JsonText::get),
            Some("340282366920938463463374607431768211455")
        );
        let stored = serde_json::to_string(&max.clone().into_stored()).unwrap();
        let read: horsies_core::TaskError = serde_json::from_str(&stored).unwrap();
        let read = TaskError::<ChargeCode>::from_stored(read).unwrap();
        assert_eq!(read.data().unwrap().decode::<u128>().unwrap(), u128::MAX);
        assert_eq!(read, max);
    }

    #[test]
    fn a_non_finite_float_is_replaced_and_the_code_is_kept() {
        for ratio in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let error = declined().with_data(Ratio { ratio });
            assert_eq!(
                data_of(&error),
                Some(json!({"data_refused": "StrictJsonError"}))
            );
            assert_eq!(error.code(), &TaskErrorCode::User(ChargeCode::CardDeclined));
            assert_eq!(error.message(), Some("card declined"));
        }
    }

    #[test]
    fn an_engine_key_is_replaced() {
        let error = declined().with_data(json!({"inner": {"__h_task_result__": 1}}));
        assert_eq!(
            data_of(&error),
            Some(json!({"data_refused": "StrictJsonError"}))
        );
    }

    #[test]
    fn data_deeper_than_the_error_bound_is_replaced() {
        let error = declined().with_data(nested(ERROR_DATA_DEPTH_LIMIT + 1));
        assert_eq!(
            data_of(&error),
            Some(json!({"data_refused": "ValueValidationError"}))
        );
        let at_bound = declined().with_data(nested(ERROR_DATA_DEPTH_LIMIT));
        assert_eq!(data_of(&at_bound), Some(nested(ERROR_DATA_DEPTH_LIMIT)));
        let past_text_bound =
            declined().with_data(nested(horsies_core::core::codec::MAX_JSON_DEPTH + 1));
        assert_eq!(
            data_of(&past_text_bound),
            Some(json!({"data_refused": "StrictJsonError"}))
        );
    }

    #[test]
    fn a_user_error_round_trips_through_the_stored_form() {
        let error = declined().with_data(json!({"attempt": 1}));
        let stored = error.clone().into_stored();
        assert_eq!(
            stored.error_code,
            horsies_core::TaskErrorCode::User(UserCode::parse("CARD_DECLINED").unwrap())
        );
        let text = serde_json::to_string(&stored).unwrap();
        let read: horsies_core::TaskError = serde_json::from_str(&text).unwrap();
        assert_eq!(TaskError::<ChargeCode>::from_stored(read).unwrap(), error);
    }

    #[test]
    fn a_library_error_reads_with_any_code_type() {
        let stored = horsies_core::TaskError::builtin(
            OperationalErrorCode::WorkerCrashed,
            "child exited with signal 9",
        );
        let read = TaskError::<std::convert::Infallible>::from_stored(stored).unwrap();
        assert_eq!(
            read.code(),
            &TaskErrorCode::Library(OperationalErrorCode::WorkerCrashed.into())
        );
    }

    #[test]
    fn a_user_code_that_the_type_does_not_name_is_refused() {
        let stored = horsies_core::TaskError::new("OUT_OF_STOCK", "no stock").unwrap();
        let refused = TaskError::<ChargeCode>::from_stored(stored).unwrap_err();
        assert_eq!(refused.code, "OUT_OF_STOCK");
        assert!(refused.code_type.contains("ChargeCode"));
    }

    #[test]
    fn a_stored_sub_workflow_failure_keeps_its_child() {
        let failure = SubWorkflowFailure {
            sub_workflow_id: uuid::Uuid::nil(),
            sub_workflow_summary: SubWorkflowSummary {
                status: horsies_core::WorkflowStatus::Failed,
                output: None,
                total_tasks: 2,
                completed_tasks: 1,
                failed_tasks: 1,
                skipped_tasks: 0,
                error_summary: Some("child task failed".to_owned()),
            },
        };
        let stored = horsies_core::TaskError {
            kind: TaskErrorKind::SubWorkflow(Box::new(failure.clone())),
            ..horsies_core::TaskError::builtin(OutcomeCode::SubworkflowFailed, "child failed")
        };
        let read = TaskError::<ChargeCode>::from_stored(stored).unwrap();
        assert_eq!(read.sub_workflow(), Some(&failure));
    }
}

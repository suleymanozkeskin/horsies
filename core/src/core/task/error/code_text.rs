//! The text rule of an error code that retry matching names (Python
//! `validate_error_code_string`, `core/exception_mapper.py:64-84`).
//!
//! Python applies the rule to the codes of `RetryPolicy.auto_retry_for`
//! (`validate_error_code_fields`, `core/models/tasks.py:593-600`), to the
//! codes of an exception mapper and to `default_unhandled_error_code`. It
//! does not apply it to the code of a `TaskError`, which may be any text that
//! is not a built-in or reserved name, such as `my_user_code`. So
//! [`super::CustomCode::parse`] does not apply it either: that parse also reads
//! the stored errors that Python tasks write.
//!
//! Callers: the stored retry policy reader of the worker
//! (`worker/retry/stored_policy.rs`) and the check of a task's
//! `auto_retry_for` in the Rust client (`clients/rust/horsies/src/check.rs`).

use super::RESERVED_NON_TASK_CODES;

/// Why a text is not an error code that retry matching can name. The checks
/// run in Python's order, and the first one that refuses gives the refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeTextRefusal {
    /// The text is empty.
    Empty,
    /// The text matches `^[A-Z][A-Za-z0-9_]*(Error|Exception)$`
    /// (`EXCEPTION_NAME_RE`).
    ExceptionClassName,
    /// The text does not match `^[A-Z][A-Z0-9_]*$` (`ERROR_CODE_RE`).
    NotUpperSnakeCase,
    /// The text is in [`RESERVED_NON_TASK_CODES`].
    ReservedName,
}

impl CodeTextRefusal {
    /// Python's text of the refusal of `code` in the field `field_name`.
    pub fn message(self, field_name: &str, code: &str) -> String {
        match self {
            Self::Empty => format!("{field_name} must be a non-empty string, got ''"),
            Self::ExceptionClassName => format!(
                "{field_name} '{code}' looks like an exception class name; retry matching is \
                 error-code-only, use UPPER_SNAKE_CASE code names"
            ),
            Self::NotUpperSnakeCase => format!(
                "{field_name} '{code}' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED)"
            ),
            Self::ReservedName => format!(
                "{field_name} '{code}' is a reserved error code name and is not a task code; use \
                 another UPPER_SNAKE_CASE code"
            ),
        }
    }
}

/// Check `code` against the text rule of an error code that retry matching
/// names. `Ok` when the text is such a code. Pure.
pub fn check_error_code_text(code: &str) -> Result<(), CodeTextRefusal> {
    match code {
        "" => Err(CodeTextRefusal::Empty),
        _ if looks_like_exception_class_name(code) => Err(CodeTextRefusal::ExceptionClassName),
        _ if !is_upper_snake_case(code) => Err(CodeTextRefusal::NotUpperSnakeCase),
        _ if RESERVED_NON_TASK_CODES.contains(&code) => Err(CodeTextRefusal::ReservedName),
        _ => Ok(()),
    }
}

/// Whether `code` matches `^[A-Z][A-Za-z0-9_]*(Error|Exception)$`: an
/// upper-case ASCII letter, then ASCII letters, digits and `_` that end in
/// `Error` or `Exception` after that first letter.
fn looks_like_exception_class_name(code: &str) -> bool {
    matches!(code.chars().next(), Some('A'..='Z'))
        && code
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        && ["Error", "Exception"]
            .into_iter()
            .any(|suffix| code.len() > suffix.len() && code.ends_with(suffix))
}

/// Whether `code` matches `^[A-Z][A-Z0-9_]*$` (ASCII).
fn is_upper_snake_case(code: &str) -> bool {
    let mut characters = code.chars();
    matches!(characters.next(), Some('A'..='Z'))
        && characters.all(|character| matches!(character, 'A'..='Z' | '0'..='9' | '_'))
}

#[cfg(test)]
mod tests {
    use super::super::{
        ContractCode, CustomCode, OperationalErrorCode, OutcomeCode, RetrievalCode, TaskErrorCode,
    };
    use super::*;
    use crate::catalog::CodeEnum;

    /// Each refusal with Python's text, as `validate_error_code_string(code,
    /// field_name='auto_retry_for')` returns it.
    #[test]
    fn each_refusal_has_python_s_text() {
        let cases = [
            (
                "",
                CodeTextRefusal::Empty,
                "auto_retry_for must be a non-empty string, got ''",
            ),
            (
                "RateLimitError",
                CodeTextRefusal::ExceptionClassName,
                "auto_retry_for 'RateLimitError' looks like an exception class name; retry \
                 matching is error-code-only, use UPPER_SNAKE_CASE code names",
            ),
            (
                "my_error",
                CodeTextRefusal::NotUpperSnakeCase,
                "auto_retry_for 'my_error' is invalid; expected UPPER_SNAKE_CASE (e.g. \
                 RATE_LIMITED)",
            ),
            (
                "WORKFLOW_STOPPED",
                CodeTextRefusal::ReservedName,
                "auto_retry_for 'WORKFLOW_STOPPED' is a reserved error code name and is not a \
                 task code; use another UPPER_SNAKE_CASE code",
            ),
        ];
        for (code, refusal, text) in cases {
            assert_eq!(check_error_code_text(code), Err(refusal), "{code:?}");
            assert_eq!(refusal.message("auto_retry_for", code), text);
        }
    }

    /// The refusal of each text, as Python's checks give it in their order:
    /// a name that ends in `Error` or `Exception` is an exception class name
    /// before it is a text that is not UPPER_SNAKE_CASE.
    #[test]
    fn the_first_check_that_refuses_gives_the_refusal() {
        let cases = [
            ("TimeoutException", CodeTextRefusal::ExceptionClassName),
            ("XError", CodeTextRefusal::ExceptionClassName),
            ("Rate_Error", CodeTextRefusal::ExceptionClassName),
            ("Error", CodeTextRefusal::NotUpperSnakeCase),
            ("Exception", CodeTextRefusal::NotUpperSnakeCase),
            ("RateError_", CodeTextRefusal::NotUpperSnakeCase),
            ("my_user_code", CodeTextRefusal::NotUpperSnakeCase),
            ("RATE-LIMITED", CodeTextRefusal::NotUpperSnakeCase),
            ("_RATE", CodeTextRefusal::NotUpperSnakeCase),
            ("9LIVES", CodeTextRefusal::NotUpperSnakeCase),
            ("RATE_LIMITED\n", CodeTextRefusal::NotUpperSnakeCase),
            ("\u{c4}RGER", CodeTextRefusal::NotUpperSnakeCase),
        ];
        for (code, refusal) in cases {
            assert_eq!(check_error_code_text(code), Err(refusal), "{code:?}");
        }
    }

    #[test]
    fn upper_snake_case_codes_pass() {
        for code in [
            "RATE_LIMITED",
            "A",
            "Z9_",
            "ERROR",
            "HTTP_ERROR",
            "RATE_LIMITED_ERROR",
        ] {
            assert_eq!(check_error_code_text(code), Ok(()), "{code:?}");
        }
    }

    /// Python checks the value of a built-in code in `auto_retry_for` too;
    /// each one passes.
    #[test]
    fn every_built_in_code_passes() {
        let values = [
            OperationalErrorCode::CATALOG.members,
            ContractCode::CATALOG.members,
            RetrievalCode::CATALOG.members,
            OutcomeCode::CATALOG.members,
        ]
        .into_iter()
        .flatten()
        .map(|member| member.value);
        for value in values {
            assert_eq!(check_error_code_text(value), Ok(()), "{value}");
        }
    }

    /// The two rules differ, as in Python: a task error may carry
    /// `my_user_code` (Python `clients/python/tests/unit/test_typed_codec.py:529`), and a
    /// stored error with that code reads, while `auto_retry_for` refuses it.
    #[test]
    fn a_task_error_code_is_not_held_to_the_retry_code_rule() {
        let code = CustomCode::parse("my_user_code").expect("a task error accepts any free text");
        assert_eq!(code.as_str(), "my_user_code");
        assert_eq!(
            serde_json::from_str::<TaskErrorCode>(r#""my_user_code""#).ok(),
            Some(TaskErrorCode::Custom(code))
        );
        assert_eq!(
            check_error_code_text("my_user_code"),
            Err(CodeTextRefusal::NotUpperSnakeCase)
        );
    }
}

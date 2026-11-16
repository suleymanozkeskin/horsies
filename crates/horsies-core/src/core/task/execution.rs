//! The execution class that every task row and every task node stores.
//!
//! Python reference: `Execution` (`horsies/core/models/tasks.py`) and
//! `stored_execution` (`horsies/core/codec/task_options.py`).

use serde_json::{Map, Value};

/// Least cancel grace of a `SHARED_ASYNC` task, in milliseconds.
pub const MIN_CANCEL_GRACE_MS: i64 = 100;

/// Greatest cancel grace of a `SHARED_ASYNC` task, in milliseconds.
pub const MAX_CANCEL_GRACE_MS: i64 = 10_000;

/// How a worker runs a task. The column `execution` holds one of these values.
///
/// The order is the order of Python's `Execution` members: `ISOLATED`,
/// `SHARED_ASYNC`, `SHARED_THREAD`. Reports list classes in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Execution {
    /// One task at a time in its own child process. The default class.
    Isolated,
    /// Many async tasks in one child process.
    SharedAsync,
    /// Many blocking tasks on threads in one child process.
    SharedThread,
}

impl Execution {
    /// The stored value: `ISOLATED`, `SHARED_ASYNC` or `SHARED_THREAD`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Isolated => "ISOLATED",
            Self::SharedAsync => "SHARED_ASYNC",
            Self::SharedThread => "SHARED_THREAD",
        }
    }

    /// The class of a stored value; `None` for a value outside the three
    /// class names.
    pub(crate) fn from_stored(value: &str) -> Option<Self> {
        match value {
            "ISOLATED" => Some(Self::Isolated),
            "SHARED_ASYNC" => Some(Self::SharedAsync),
            "SHARED_THREAD" => Some(Self::SharedThread),
            _ => None,
        }
    }
}

/// Task options whose `execution` or `cancel_grace_ms` does not validate.
///
/// `detail` names the field and the cause. It holds no option value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{detail}")]
pub struct InvalidExecution {
    pub detail: String,
}

/// The execution class that enqueue stores for decoded task options.
///
/// - No options (`None`) store `ISOLATED`.
/// - Options without an `execution` key store `ISOLATED`.
/// - A present `execution` must be one of the three class names.
/// - `cancel_grace_ms` must fit the class: an integer from
///   [`MIN_CANCEL_GRACE_MS`] to [`MAX_CANCEL_GRACE_MS`] for `SHARED_ASYNC`;
///   absent or `null` for the other classes.
///
/// Other option fields are not checked. Does no I/O.
pub fn stored_execution(
    options: Option<&Map<String, Value>>,
) -> Result<Execution, InvalidExecution> {
    let Some(options) = options else {
        return Ok(Execution::Isolated);
    };
    let execution = stored_execution_class(options)?;
    match grace_error(execution, options.get("cancel_grace_ms")) {
        None => Ok(execution),
        Some(detail) => Err(InvalidExecution { detail }),
    }
}

fn stored_execution_class(options: &Map<String, Value>) -> Result<Execution, InvalidExecution> {
    match options.get("execution") {
        None => Ok(Execution::Isolated),
        Some(Value::String(raw)) => Execution::from_stored(raw).ok_or_else(|| InvalidExecution {
            detail: "execution is not one of ISOLATED, SHARED_ASYNC, SHARED_THREAD".to_owned(),
        }),
        Some(other) => Err(InvalidExecution {
            detail: format!(
                "execution is a JSON {}, not a string",
                json_type_name(other)
            ),
        }),
    }
}

fn grace_error(execution: Execution, raw: Option<&Value>) -> Option<String> {
    match execution {
        Execution::SharedAsync => shared_async_grace_error(raw),
        Execution::Isolated | Execution::SharedThread => match raw {
            None | Some(Value::Null) => None,
            Some(_) => Some(format!(
                "cancel_grace_ms is set for execution {}; only SHARED_ASYNC has one",
                execution.as_str(),
            )),
        },
    }
}

fn shared_async_grace_error(raw: Option<&Value>) -> Option<String> {
    match raw {
        None | Some(Value::Null) => {
            Some("cancel_grace_ms is absent for execution SHARED_ASYNC".to_owned())
        }
        Some(Value::Bool(_)) => Some("cancel_grace_ms is a bool, not an int".to_owned()),
        Some(Value::Number(number)) => match number.as_i64() {
            Some(ms) if ms < MIN_CANCEL_GRACE_MS => {
                Some(format!("cancel_grace_ms is below {MIN_CANCEL_GRACE_MS} ms"))
            }
            Some(ms) if ms > MAX_CANCEL_GRACE_MS => {
                Some(format!("cancel_grace_ms is above {MAX_CANCEL_GRACE_MS} ms"))
            }
            Some(_) => None,
            None if number.is_u64() => {
                Some(format!("cancel_grace_ms is above {MAX_CANCEL_GRACE_MS} ms"))
            }
            None => Some("cancel_grace_ms is a JSON float, not an int".to_owned()),
        },
        Some(other) => Some(format!(
            "cancel_grace_ms is a JSON {}, not an int",
            json_type_name(other)
        )),
    }
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(value: Value) -> Map<String, Value> {
        match value {
            Value::Object(map) => map,
            other => panic!("test options must be an object, got {other}"),
        }
    }

    #[test]
    fn absent_options_and_absent_key_store_isolated() {
        assert_eq!(stored_execution(None), Ok(Execution::Isolated));
        let empty = options(serde_json::json!({}));
        assert_eq!(stored_execution(Some(&empty)), Ok(Execution::Isolated));
    }

    #[test]
    fn each_class_name_is_stored() {
        let isolated = options(serde_json::json!({"execution": "ISOLATED"}));
        let thread = options(serde_json::json!({"execution": "SHARED_THREAD"}));
        let shared_async =
            options(serde_json::json!({"execution": "SHARED_ASYNC", "cancel_grace_ms": 1000}));
        assert_eq!(stored_execution(Some(&isolated)), Ok(Execution::Isolated));
        assert_eq!(stored_execution(Some(&thread)), Ok(Execution::SharedThread));
        assert_eq!(
            stored_execution(Some(&shared_async)),
            Ok(Execution::SharedAsync)
        );
    }

    #[test]
    fn unknown_class_and_non_string_class_are_refused() {
        for value in [
            serde_json::json!({"execution": "PARALLEL"}),
            serde_json::json!({"execution": null}),
            serde_json::json!({"execution": 1}),
        ] {
            let refused = options(value);
            assert!(stored_execution(Some(&refused)).is_err());
        }
    }

    #[test]
    fn cancel_grace_must_fit_the_class() {
        for value in [
            serde_json::json!({"execution": "SHARED_ASYNC"}),
            serde_json::json!({"execution": "SHARED_ASYNC", "cancel_grace_ms": 99}),
            serde_json::json!({"execution": "SHARED_ASYNC", "cancel_grace_ms": 10_001}),
            serde_json::json!({"execution": "SHARED_ASYNC", "cancel_grace_ms": 1.5}),
            serde_json::json!({"execution": "SHARED_ASYNC", "cancel_grace_ms": true}),
            serde_json::json!({"execution": "ISOLATED", "cancel_grace_ms": 1000}),
            serde_json::json!({"cancel_grace_ms": 1000}),
        ] {
            let refused = options(value);
            assert!(stored_execution(Some(&refused)).is_err());
        }
        let thread_null =
            options(serde_json::json!({"execution": "SHARED_THREAD", "cancel_grace_ms": null}));
        assert_eq!(
            stored_execution(Some(&thread_null)),
            Ok(Execution::SharedThread)
        );
    }
}

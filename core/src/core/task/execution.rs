//! The execution class that every task row and every task node stores, and
//! the class that task info reports.
//!
//! Python reference: `Execution` and `TaskInfo.execution`
//! (`clients/python/horsies/core/models/tasks.py`), and `stored_execution`
//! (`clients/python/horsies/core/codec/task_options.py`).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Least cancel grace of a `SHARED_ASYNC` or an `ISOLATED` task, in
/// milliseconds.
pub const MIN_CANCEL_GRACE_MS: i64 = 100;

/// Greatest cancel grace of a `SHARED_ASYNC` or an `ISOLATED` task, in
/// milliseconds.
pub const MAX_CANCEL_GRACE_MS: i64 = 10_000;

/// How a worker runs a task. The column `execution` holds one of these values.
/// The executor of the worker uses this type too (`ExecutionClass` there):
/// the class of a row, the budget of a claim and the kind of a child are
/// one set of values.
///
/// The order is the order of Python's `Execution` members: `ISOLATED`,
/// `SHARED_ASYNC`, `SHARED_THREAD`. Reports list classes in this order. It
/// serializes as its stored value.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    horsies_core_derive::ManifestSchema,
    horsies_core_derive::EventSchema,
)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Execution {
    /// One task at a time in its own child process. The default class.
    Isolated,
    /// Many async tasks in one child process.
    SharedAsync,
    /// Many blocking tasks on threads in one child process.
    SharedThread,
}

impl Execution {
    /// Every class, in the order of the stored values.
    pub const ALL: [Self; 3] = [Self::Isolated, Self::SharedAsync, Self::SharedThread];

    /// The stored value: `ISOLATED`, `SHARED_ASYNC` or `SHARED_THREAD`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Isolated => "ISOLATED",
            Self::SharedAsync => "SHARED_ASYNC",
            Self::SharedThread => "SHARED_THREAD",
        }
    }

    /// Whether the class needs a `timeout_ms`. One fact for the check of a
    /// declaration (`check_declared_options`) and the check of a new send
    /// (`check_shared_timeout`).
    pub fn timeout_use(self) -> TimeoutUse {
        match self {
            Self::Isolated => TimeoutUse::Optional,
            Self::SharedAsync | Self::SharedThread => TimeoutUse::Required,
        }
    }

    /// What the class does with a `cancel_grace_ms`. One fact for the check
    /// of a declaration (`check_declared_options`) and the check of stored
    /// options at enqueue (`stored_execution`).
    pub fn grace_use(self) -> GraceUse {
        match self {
            Self::SharedAsync => GraceUse::Needed,
            Self::Isolated => GraceUse::Optional,
            Self::SharedThread => GraceUse::NotTaken,
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

/// Whether a class needs a `timeout_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutUse {
    /// `SHARED_ASYNC` and `SHARED_THREAD`: a shared child runs other tasks
    /// next to this one, and the timeout ends a task that does not return.
    Required,
    /// `ISOLATED`: the task runs with or without a timeout.
    Optional,
}

/// What a class does with a `cancel_grace_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraceUse {
    /// `SHARED_ASYNC`: a grace from [`MIN_CANCEL_GRACE_MS`] to
    /// [`MAX_CANCEL_GRACE_MS`]. A declaration with none takes the default
    /// grace; a stored row holds the grace.
    Needed,
    /// `ISOLATED`: a grace in the same bounds (the time from an operator
    /// cancel to the kill of the child), or none.
    Optional,
    /// `SHARED_THREAD`: no grace, since a thread cannot be stopped.
    NotTaken,
}

impl std::fmt::Display for Execution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The reported value of [`ExecutionClass::NotKeptInHistory`].
const NOT_KEPT_IN_HISTORY: &str = "NOT_KEPT_IN_HISTORY";

/// The execution class that task info reports for one task.
///
/// A live task reports the class in its `execution` column. A task read from
/// history reports [`ExecutionClass::NotKeptInHistory`]: the history table
/// has no `execution` column, as in Python, where `TaskInfo.execution` is
/// `None` for a history read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionClass {
    /// A live task: the class stored in its `execution` column.
    Live(Execution),
    /// A task read from history. The history table does not keep the class.
    NotKeptInHistory,
}

impl ExecutionClass {
    /// The reported value: the stored class name of a live task (`ISOLATED`,
    /// `SHARED_ASYNC` or `SHARED_THREAD`), or `NOT_KEPT_IN_HISTORY`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Live(execution) => execution.as_str(),
            Self::NotKeptInHistory => NOT_KEPT_IN_HISTORY,
        }
    }

    /// The class of a reported value; `None` for any other text.
    fn from_reported(value: &str) -> Option<Self> {
        match value {
            NOT_KEPT_IN_HISTORY => Some(Self::NotKeptInHistory),
            stored => Execution::from_stored(stored).map(Self::Live),
        }
    }
}

impl serde::Serialize for ExecutionClass {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for ExecutionClass {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_reported(&value).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "execution is not one of ISOLATED, SHARED_ASYNC, SHARED_THREAD, {}: {}",
                NOT_KEPT_IN_HISTORY, value,
            ))
        })
    }
}

/// Task options whose `execution` or `cancel_grace_ms` does not validate, or
/// whose shared class has no `timeout_ms` (`check_shared_timeout`).
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
///   absent, `null`, or such an integer for `ISOLATED` (the time from an
///   operator cancel to the kill of the child; none means no kill); absent
///   or `null` for `SHARED_THREAD`, whose thread cannot be stopped.
///
/// Other option fields are not checked: a new send checks the `timeout_ms`
/// of a shared class with `check_shared_timeout`. Does no I/O.
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
    match (execution.grace_use(), raw) {
        (GraceUse::Needed, None | Some(Value::Null)) => Some(format!(
            "cancel_grace_ms is absent for execution {execution}"
        )),
        (GraceUse::Needed | GraceUse::Optional, Some(value)) => bounded_grace_error(value),
        (GraceUse::Optional | GraceUse::NotTaken, None | Some(Value::Null)) => None,
        (GraceUse::NotTaken, Some(_)) => Some(format!(
            "cancel_grace_ms is set for execution {execution}; only SHARED_ASYNC and \
             ISOLATED have one"
        )),
    }
}

/// The rule of a present `cancel_grace_ms` that is not `null`: an integer
/// from [`MIN_CANCEL_GRACE_MS`] to [`MAX_CANCEL_GRACE_MS`].
fn bounded_grace_error(raw: &Value) -> Option<String> {
    match raw {
        Value::Null => None,
        Value::Bool(_) => Some("cancel_grace_ms is a bool, not an int".to_owned()),
        Value::Number(number) => match number.as_i64() {
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
        other @ (Value::String(_) | Value::Array(_) | Value::Object(_)) => Some(format!(
            "cancel_grace_ms is a JSON {}, not an int",
            json_type_name(other)
        )),
    }
}

/// The `timeout_ms` rule of the class `execution` at a new send
/// (`CheckedSend::check`) and at a workflow start (`prepare_workflow_task_facts`): `SHARED_ASYNC` and `SHARED_THREAD` need a timeout,
/// because a shared child runs other tasks next to this one and the timeout
/// ends a task that does not return. `ISOLATED` runs with or without one.
/// The Rust task macro and the Python task decorator refuse the same
/// declaration at registration. A schedule and a rerun do not apply this
/// rule: they write such options, and the worker refuses the run.
///
/// `timeout_ms` is the raw member of the decoded options: absent (`None`)
/// and `null` are no timeout. A present value is not judged here; the
/// dispatch read of the stored options (`read_stored_options`) checks its
/// type and its bounds. Does no I/O.
///
/// Errors: [`InvalidExecution`] for a shared class with no timeout.
pub(crate) fn check_shared_timeout(
    execution: Execution,
    timeout_ms: Option<&Value>,
) -> Result<(), InvalidExecution> {
    match (execution.timeout_use(), timeout_ms) {
        (TimeoutUse::Required, None | Some(Value::Null)) => Err(InvalidExecution {
            detail: format!(
                "timeout_ms is absent for execution {execution}; a shared class requires \
                 timeout_ms"
            ),
        }),
        (TimeoutUse::Required, Some(_)) | (TimeoutUse::Optional, _) => Ok(()),
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
            serde_json::json!({"execution": "SHARED_THREAD", "cancel_grace_ms": 1000}),
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

    /// An `ISOLATED` task takes a cancel grace inside the bounds (the time
    /// from an operator cancel to the kill of its child), or none.
    #[test]
    fn an_isolated_task_takes_a_bounded_cancel_grace_or_none() {
        for value in [
            serde_json::json!({"execution": "ISOLATED", "cancel_grace_ms": 100}),
            serde_json::json!({"execution": "ISOLATED", "cancel_grace_ms": 10_000}),
            serde_json::json!({"execution": "ISOLATED", "cancel_grace_ms": null}),
            serde_json::json!({"execution": "ISOLATED"}),
            serde_json::json!({"cancel_grace_ms": 1000}),
        ] {
            let accepted = options(value);
            assert_eq!(stored_execution(Some(&accepted)), Ok(Execution::Isolated));
        }
        for (value, detail) in [
            (
                serde_json::json!({"execution": "ISOLATED", "cancel_grace_ms": 99}),
                "cancel_grace_ms is below 100 ms",
            ),
            (
                serde_json::json!({"execution": "ISOLATED", "cancel_grace_ms": 10_001}),
                "cancel_grace_ms is above 10000 ms",
            ),
            (
                serde_json::json!({"execution": "ISOLATED", "cancel_grace_ms": "500"}),
                "cancel_grace_ms is a JSON str, not an int",
            ),
        ] {
            let refused = options(value);
            assert_eq!(
                stored_execution(Some(&refused)),
                Err(InvalidExecution {
                    detail: detail.to_owned()
                })
            );
        }
    }

    /// A `SHARED_THREAD` task takes no cancel grace: its thread cannot be
    /// stopped.
    #[test]
    fn a_shared_thread_task_takes_no_cancel_grace() {
        let refused =
            options(serde_json::json!({"execution": "SHARED_THREAD", "cancel_grace_ms": 1000}));
        assert_eq!(
            stored_execution(Some(&refused)),
            Err(InvalidExecution {
                detail: "cancel_grace_ms is set for execution SHARED_THREAD; only SHARED_ASYNC \
                         and ISOLATED have one"
                    .to_owned()
            })
        );
    }

    /// A shared class with no `timeout_ms` is refused: an absent member and
    /// `null` are both no timeout.
    #[test]
    fn a_shared_class_without_a_timeout_is_refused() {
        let null = Value::Null;
        for (execution, detail) in [
            (
                Execution::SharedAsync,
                "timeout_ms is absent for execution SHARED_ASYNC; a shared class requires \
                 timeout_ms",
            ),
            (
                Execution::SharedThread,
                "timeout_ms is absent for execution SHARED_THREAD; a shared class requires \
                 timeout_ms",
            ),
        ] {
            for timeout_ms in [None, Some(&null)] {
                assert_eq!(
                    check_shared_timeout(execution, timeout_ms),
                    Err(InvalidExecution {
                        detail: detail.to_owned()
                    }),
                    "{execution} {timeout_ms:?}"
                );
            }
        }
    }

    /// `ISOLATED` runs with or without a timeout, and a shared class with a
    /// timeout passes the rule.
    #[test]
    fn an_isolated_class_or_a_shared_class_with_a_timeout_passes() {
        let null = Value::Null;
        let timeout = serde_json::json!(30_000);
        for (execution, timeout_ms) in [
            (Execution::Isolated, None),
            (Execution::Isolated, Some(&null)),
            (Execution::Isolated, Some(&timeout)),
            (Execution::SharedAsync, Some(&timeout)),
            (Execution::SharedThread, Some(&timeout)),
        ] {
            assert_eq!(
                check_shared_timeout(execution, timeout_ms),
                Ok(()),
                "{execution} {timeout_ms:?}"
            );
        }
    }

    /// The class facts that the declaration check and the stored-options
    /// checks read: a shared class needs a timeout; `SHARED_ASYNC` needs a
    /// grace, `ISOLATED` may take one and `SHARED_THREAD` takes none.
    #[test]
    fn each_class_has_its_timeout_use_and_its_grace_use() {
        assert_eq!(
            Execution::ALL.map(Execution::timeout_use),
            [
                TimeoutUse::Optional,
                TimeoutUse::Required,
                TimeoutUse::Required
            ]
        );
        assert_eq!(
            Execution::ALL.map(Execution::grace_use),
            [GraceUse::Optional, GraceUse::Needed, GraceUse::NotTaken]
        );
    }

    #[test]
    fn execution_class_reports_the_live_class_or_not_kept_in_history() {
        for (class, reported) in [
            (ExecutionClass::Live(Execution::Isolated), "ISOLATED"),
            (ExecutionClass::Live(Execution::SharedAsync), "SHARED_ASYNC"),
            (
                ExecutionClass::Live(Execution::SharedThread),
                "SHARED_THREAD",
            ),
            (ExecutionClass::NotKeptInHistory, "NOT_KEPT_IN_HISTORY"),
        ] {
            assert_eq!(class.as_str(), reported);
            let encoded = serde_json::to_value(class).unwrap();
            assert_eq!(encoded, Value::String(reported.to_owned()));
            let decoded: ExecutionClass = serde_json::from_value(encoded).unwrap();
            assert_eq!(decoded, class);
        }
    }

    #[test]
    fn execution_class_refuses_other_values() {
        for refused in [
            serde_json::json!("PARALLEL"),
            serde_json::json!("isolated"),
            serde_json::json!(null),
            serde_json::json!(1),
        ] {
            assert!(serde_json::from_value::<ExecutionClass>(refused).is_err());
        }
    }
}

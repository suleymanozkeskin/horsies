/// Typed error types for PostgresBroker operations.
///
/// Mirrors Python's `python/horsies/core/brokers/result_types.py`.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::core::codec::{check_json_text, StrictJsonError};
use crate::core::history::reads::detail::TaskAbsenceReason;
use crate::core::{TaskInfo, TaskStatus};

/// Categorized broker operation failure codes.
///
/// The members are Python's, in Python's order
/// (`python/horsies/core/brokers/result_types.py`). The nine operation codes from
/// `SCHEMA_INIT_FAILED` to `LISTENER_SUBSCRIBE_FAILED` share one variant in
/// Python, `BrokerOperationFailed`. A client produces `NO_BROKER` and
/// `NO_TYPE_AVAILABLE`; only the Python client produces `EVENT_LOOP_LIMIT`,
/// `ASYNC_CONTEXT` and `LOOP_RUNNER_FAILED`.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, horsies_macros::CodeCatalog,
)]
#[code_catalog(module = "broker", base = "str_enum")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BrokerErrorCode {
    SchemaInitFailed,
    EnqueueFailed,
    PayloadMismatch,
    /// The idempotency key is reserved by another task with a different
    /// command. Nothing was written.
    IdempotencyKeyConflict,
    TaskInfoQueryFailed,
    MonitoringQueryFailed,
    DbPingFailed,
    WorkerPingFailed,
    CleanupFailed,
    CloseFailed,
    ListenerStartFailed,
    ListenerSubscribeFailed,
    /// The task handle has no broker, so it cannot read task info. Nothing
    /// ran. A client produces it.
    NoBroker,
    /// A typed result was requested, but the process has no registered type
    /// for the task. The stored result is unchanged. A client produces it.
    NoTypeAvailable,
    /// Stored or wire JSON failed strict validation.
    InvalidJsonPayload,
    /// The task options passed to enqueue are not valid JSON, or carry a
    /// retry limit that cannot be stored; nothing was written.
    InvalidTaskOptions,
    /// The broker already holds resource sets for its limit of open event
    /// loops, and this call ran on a new one. Nothing ran. Python client
    /// only; removed before 0.8.0.
    EventLoopLimit,
    /// A sync facade was called on a thread that runs an event loop. Its wait
    /// would stall that loop, so nothing ran. Python client only.
    AsyncContext,
    /// A sync facade could not run the call on the broker's sync runner.
    /// Nothing ran. Python client only; removed before 0.8.0.
    LoopRunnerFailed,
    /// An argument of the call was refused before anything ran. Nothing was
    /// written, and the same arguments are refused again.
    InvalidInput,
    /// The broker has no listener, so an operation that needs one cannot run.
    /// Nothing ran. It does not clear for this broker.
    ListenerDisabled,
    /// A result wait was called in a process that runs task bodies. A wait
    /// there can hold the slot that the awaited task needs, so nothing ran.
    WaitInsideTask,
}

impl std::fmt::Display for BrokerErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = serde_plain::to_string(self).unwrap_or_else(|_| format!("{:?}", self));
        write!(f, "{}", s)
    }
}

/// Error payload for broker operations.
///
/// Carried inside `Err(...)` of a `BrokerResult<T>`.
#[derive(Debug, Clone)]
pub struct BrokerOperationError {
    /// Which operation category failed.
    pub code: BrokerErrorCode,
    /// Human-readable description.
    pub message: String,
    /// Whether the caller can retry this operation.
    pub retryable: bool,
}

impl std::fmt::Display for BrokerOperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} (retryable={})",
            self.code, self.message, self.retryable,
        )
    }
}

impl std::error::Error for BrokerOperationError {}

/// Result type for broker operations.
///
/// `Ok(T)` on success, `Err(BrokerOperationError)` on infrastructure failure.
pub type BrokerResult<T> = Result<T, BrokerOperationError>;

/// Raw broker-level snapshot of a task's stored result envelope.
///
/// The broker verifies history envelopes and checks that the stored text is
/// a JSON object, but deliberately performs no task-specific `Ok` decoding.
/// Typed handles layer their task result type on top of this record.
#[derive(Debug, Clone, PartialEq)]
pub struct RawResultRecord {
    pub task_id: Uuid,
    pub task_name: String,
    pub status: TaskStatus,
    pub raw_result: Option<StoredEnvelopeText>,
}

/// The stored result envelope of a task, as the text that the row stores.
///
/// The text passed the strict read rule and is one JSON object. It is not
/// parsed into a value: a `serde_json::Value` holds an integer outside the
/// `i64` and `u64` range as an `f64`, so Python's text of `2**64` would read
/// back as a float (`tests/fixtures/stored_forms/README.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEnvelopeText(String);

/// Why a stored result text is not a result envelope text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoredEnvelopeRefused {
    /// The text breaks the strict read rule.
    #[error("result JSON is refused: {0}")]
    NotStrictJson(#[from] StrictJsonError),
    /// The text is another JSON value than an object or `null`; the kind is
    /// named.
    #[error("result JSON must be an object, got {0}")]
    NotAnObject(&'static str),
}

impl StoredEnvelopeText {
    /// Read a stored result text: `None` for the JSON `null`, which is no
    /// result; the text for a JSON object; a refusal for a text that breaks
    /// the strict read rule or holds another JSON value. Pure.
    pub fn parse(text: String) -> Result<Option<Self>, StoredEnvelopeRefused> {
        check_json_text(&text)?;
        // The strict read passed, so the first byte after the whitespace
        // starts the one JSON value of the text.
        match text.trim_start().as_bytes().first() {
            Some(b'{') => Ok(Some(Self(text))),
            Some(b'n') => Ok(None),
            Some(b't' | b'f') => Err(StoredEnvelopeRefused::NotAnObject("boolean")),
            Some(b'"') => Err(StoredEnvelopeRefused::NotAnObject("string")),
            Some(b'[') => Err(StoredEnvelopeRefused::NotAnObject("array")),
            Some(_) => Err(StoredEnvelopeRefused::NotAnObject("number")),
            None => unreachable!("the strict read refuses a text that holds no value"),
        }
    }

    /// The envelope text, as stored.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What a raw result read found: the record of the task, or why neither
/// the live table nor the task history holds it.
#[derive(Debug, Clone, PartialEq)]
pub enum RawResultRead {
    Found(RawResultRecord),
    /// `reason` tells a task that retention can have removed from an id
    /// that no retained history held.
    Absent {
        reason: TaskAbsenceReason,
    },
}

#[cfg(test)]
impl RawResultRead {
    /// The record of a read that found the task. Panics on an absent task.
    pub(crate) fn expect_found(self) -> RawResultRecord {
        match self {
            Self::Found(record) => record,
            Self::Absent { reason } => panic!("the task is absent: {}", reason.as_str()),
        }
    }

    pub(crate) fn is_absent(&self) -> bool {
        matches!(self, Self::Absent { .. })
    }
}

/// What a task info read found: the info of the task, or why neither the
/// live table nor the task history holds it.
#[derive(Debug, Clone)]
pub enum TaskInfoRead {
    /// Boxed: the info is many times larger than the reason. The stored
    /// result is the envelope text as stored, with no typed decode.
    Found(Box<TaskInfo<StoredEnvelopeText>>),
    /// `reason` as in [`RawResultRead::Absent`].
    Absent { reason: TaskAbsenceReason },
}

#[cfg(test)]
impl TaskInfoRead {
    /// The info of a read that found the task. Panics on an absent task.
    pub(crate) fn expect_found(self) -> TaskInfo<StoredEnvelopeText> {
        match self {
            Self::Found(info) => *info,
            Self::Absent { reason } => panic!("the task is absent: {}", reason.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_error_code_serde() {
        let code = BrokerErrorCode::EnqueueFailed;
        let json = serde_json::to_string(&code).unwrap();
        assert_eq!(json, "\"ENQUEUE_FAILED\"");
        let back: BrokerErrorCode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, code);
    }

    #[test]
    fn broker_operation_error_display() {
        let err = BrokerOperationError {
            code: BrokerErrorCode::SchemaInitFailed,
            message: "connection refused".to_owned(),
            retryable: true,
        };
        let s = format!("{}", err);
        assert!(s.contains("SCHEMA_INIT_FAILED"));
        assert!(s.contains("connection refused"));
        assert!(s.contains("retryable=true"));
    }

    #[test]
    fn invalid_input_has_python_wire_name() {
        assert_eq!(
            serde_json::to_string(&BrokerErrorCode::InvalidInput).unwrap(),
            "\"INVALID_INPUT\""
        );
        assert_eq!(BrokerErrorCode::InvalidInput.to_string(), "INVALID_INPUT");
    }

    #[test]
    fn listener_disabled_has_python_wire_name() {
        assert_eq!(
            serde_json::to_string(&BrokerErrorCode::ListenerDisabled).unwrap(),
            "\"LISTENER_DISABLED\""
        );
    }

    #[test]
    fn all_codes_round_trip() {
        let codes = [
            BrokerErrorCode::SchemaInitFailed,
            BrokerErrorCode::EnqueueFailed,
            BrokerErrorCode::PayloadMismatch,
            BrokerErrorCode::IdempotencyKeyConflict,
            BrokerErrorCode::TaskInfoQueryFailed,
            BrokerErrorCode::MonitoringQueryFailed,
            BrokerErrorCode::DbPingFailed,
            BrokerErrorCode::WorkerPingFailed,
            BrokerErrorCode::CleanupFailed,
            BrokerErrorCode::CloseFailed,
            BrokerErrorCode::ListenerStartFailed,
            BrokerErrorCode::ListenerSubscribeFailed,
            BrokerErrorCode::NoBroker,
            BrokerErrorCode::NoTypeAvailable,
            BrokerErrorCode::InvalidJsonPayload,
            BrokerErrorCode::InvalidTaskOptions,
            BrokerErrorCode::EventLoopLimit,
            BrokerErrorCode::AsyncContext,
            BrokerErrorCode::LoopRunnerFailed,
            BrokerErrorCode::InvalidInput,
            BrokerErrorCode::ListenerDisabled,
            BrokerErrorCode::WaitInsideTask,
        ];
        for code in codes {
            let json = serde_json::to_string(&code).unwrap();
            let back: BrokerErrorCode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, code);
        }
    }
}

#[cfg(test)]
mod stored_envelope_tests {
    use super::*;

    /// The text is kept as stored: an integer above `u128::MAX` keeps every
    /// digit, and the member order and spacing stay.
    #[test]
    fn an_object_is_kept_as_its_stored_text() {
        let text = r#"{"__h_task_result__":true, "ok":340282366920938463463374607431768211456,"err":null}"#;
        let envelope = StoredEnvelopeText::parse(text.to_owned())
            .unwrap()
            .expect("an object is an envelope");
        assert_eq!(envelope.as_str(), text);
    }

    #[test]
    fn the_json_null_is_no_result() {
        assert_eq!(StoredEnvelopeText::parse(" null".to_owned()), Ok(None));
    }

    #[test]
    fn another_json_value_is_refused_with_its_kind() {
        for (text, kind) in [
            ("3", "number"),
            ("-1.5", "number"),
            (r#""x""#, "string"),
            ("[]", "array"),
            ("true", "boolean"),
            (" false", "boolean"),
        ] {
            assert_eq!(
                StoredEnvelopeText::parse(text.to_owned()),
                Err(StoredEnvelopeRefused::NotAnObject(kind)),
                "{text}"
            );
        }
    }

    #[test]
    fn a_text_that_breaks_the_strict_rule_is_refused() {
        assert!(matches!(
            StoredEnvelopeText::parse(r#"{"ok":1,"ok":2}"#.to_owned()),
            Err(StoredEnvelopeRefused::NotStrictJson(
                StrictJsonError::DuplicateKey { .. }
            ))
        ));
    }
}

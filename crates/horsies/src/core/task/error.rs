use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::core::workflow::summary::SubWorkflowSummary;

// =============================================================================
// Built-in error code families (matches Python's 4-family structure)
// =============================================================================

/// Something actually broke in execution.
///
/// Typical caller behavior: log, retry conditionally, inspect infra/runtime failure.
///
/// Note: The Rust port uses different names than Python for two variants:
///
/// - Python `UNHANDLED_EXCEPTION` → Rust `UNHANDLED_ERROR`
/// - Python `TASK_EXCEPTION`      → Rust `TASK_ERROR`
///
/// Rust has no exceptions — tasks return `Result<T, TaskError>`. "Exception"
/// is a Python-specific concept. Since the Rust port uses its own database
/// (not shared with Python), wire-format compatibility is not required.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationalErrorCode {
    /// Task panicked or produced an unexpected error the worker couldn't handle.
    /// Python equivalent: `UNHANDLED_EXCEPTION`.
    UnhandledError,
    /// Task returned `Err(TaskError)` during execution.
    /// Python equivalent: `TASK_EXCEPTION`.
    TaskError,
    WorkerCrashed,
    BrokerError,
    WorkerResolutionError,
    WorkerSerializationError,
    ResultDeserializationError,
    WorkflowEnqueueFailed,
    SubworkflowLoadFailed,
}

/// A code/typing/usage contract was violated.
///
/// Typical caller behavior: fix code or type assumptions, do not blindly retry.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContractCode {
    ArgumentTypeMismatch,
    ReturnTypeMismatch,
    /// Rust equivalent of Python's PYDANTIC_HYDRATION_ERROR.
    PydanticHydrationError,
    WorkflowCtxMissingId,
}

/// The caller cannot get the value right now or cannot locate it.
///
/// Typical caller behavior: wait, poll again, handle missing value as normal flow.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RetrievalCode {
    WaitTimeout,
    TaskNotFound,
    WorkflowNotFound,
    ResultNotAvailable,
    ResultNotReady,
}

/// A non-bug execution outcome or control-flow result.
///
/// Typical caller behavior: branch on outcome, continue control flow.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OutcomeCode {
    TaskCancelled,
    TaskExpired,
    WorkflowPaused,
    WorkflowFailed,
    WorkflowCancelled,
    UpstreamSkipped,
    SubworkflowFailed,
    WorkflowSuccessCaseNotMet,
    WorkflowStopped,
    SendSuppressed,
}

impl std::fmt::Display for OperationalErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_plain::to_string(self).unwrap_or_else(|_| format!("{:?}", self))
        )
    }
}

impl std::fmt::Display for ContractCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_plain::to_string(self).unwrap_or_else(|_| format!("{:?}", self))
        )
    }
}

impl std::fmt::Display for RetrievalCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_plain::to_string(self).unwrap_or_else(|_| format!("{:?}", self))
        )
    }
}

impl std::fmt::Display for OutcomeCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_plain::to_string(self).unwrap_or_else(|_| format!("{:?}", self))
        )
    }
}

// =============================================================================
// BuiltInTaskCode — union of all 4 families
// =============================================================================

/// Union of all built-in error code families.
///
/// Matches Python's `BuiltInTaskCode = OperationalErrorCode | ContractCode | RetrievalCode | OutcomeCode`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BuiltInTaskCode {
    Operational(OperationalErrorCode),
    Contract(ContractCode),
    Retrieval(RetrievalCode),
    Outcome(OutcomeCode),
}

impl BuiltInTaskCode {
    /// Get the string value of the error code (e.g. "BROKER_ERROR").
    pub fn value(&self) -> String {
        match self {
            Self::Operational(c) => {
                serde_plain::to_string(c).unwrap_or_else(|_| format!("{:?}", c))
            }
            Self::Contract(c) => serde_plain::to_string(c).unwrap_or_else(|_| format!("{:?}", c)),
            Self::Retrieval(c) => serde_plain::to_string(c).unwrap_or_else(|_| format!("{:?}", c)),
            Self::Outcome(c) => serde_plain::to_string(c).unwrap_or_else(|_| format!("{:?}", c)),
        }
    }

    /// Try to parse a string into a BuiltInTaskCode.
    /// Returns None if the string does not match any known code.
    pub fn parse(s: &str) -> Option<Self> {
        if let Ok(c) = serde_plain::from_str::<OperationalErrorCode>(s) {
            return Some(Self::Operational(c));
        }
        if let Ok(c) = serde_plain::from_str::<ContractCode>(s) {
            return Some(Self::Contract(c));
        }
        if let Ok(c) = serde_plain::from_str::<RetrievalCode>(s) {
            return Some(Self::Retrieval(c));
        }
        if let Ok(c) = serde_plain::from_str::<OutcomeCode>(s) {
            return Some(Self::Outcome(c));
        }
        None
    }
}

impl std::fmt::Display for BuiltInTaskCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}

/// Serializes as `{"__builtin_task_code__": "BROKER_ERROR"}`.
impl Serialize for BuiltInTaskCode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("__builtin_task_code__", &self.value())?;
        map.end()
    }
}

/// Deserializes from `{"__builtin_task_code__": "BROKER_ERROR"}`.
impl<'de> Deserialize<'de> for BuiltInTaskCode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Tagged {
            __builtin_task_code__: String,
        }
        let tagged = Tagged::deserialize(deserializer)?;
        BuiltInTaskCode::parse(&tagged.__builtin_task_code__).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "unknown built-in task code: {:?}",
                tagged.__builtin_task_code__,
            ))
        })
    }
}

// Convenience From impls
impl From<OperationalErrorCode> for BuiltInTaskCode {
    fn from(c: OperationalErrorCode) -> Self {
        Self::Operational(c)
    }
}
impl From<ContractCode> for BuiltInTaskCode {
    fn from(c: ContractCode) -> Self {
        Self::Contract(c)
    }
}
impl From<RetrievalCode> for BuiltInTaskCode {
    fn from(c: RetrievalCode) -> Self {
        Self::Retrieval(c)
    }
}
impl From<OutcomeCode> for BuiltInTaskCode {
    fn from(c: OutcomeCode) -> Self {
        Self::Outcome(c)
    }
}

// =============================================================================
// TaskErrorCode — either built-in or user-defined
// =============================================================================

/// Error code discriminator: either a built-in code (4 families) or a user-defined string.
///
/// Serialization:
/// - Built-in: `{"__builtin_task_code__": "BROKER_ERROR"}`
/// - User: `"MY_CUSTOM_ERROR"`
///
/// This tagged format ensures round-trip identity and matches Python's wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskErrorCode {
    BuiltIn(BuiltInTaskCode),
    User(String),
}

impl std::fmt::Display for TaskErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BuiltIn(code) => write!(f, "{}", code),
            Self::User(code) => write!(f, "{}", code),
        }
    }
}

impl Serialize for TaskErrorCode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::BuiltIn(code) => code.serialize(serializer),
            Self::User(code) => serializer.serialize_str(code),
        }
    }
}

impl<'de> Deserialize<'de> for TaskErrorCode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        match &value {
            // Tagged dict → built-in code
            serde_json::Value::Object(map) if map.contains_key("__builtin_task_code__") => {
                let code: BuiltInTaskCode =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(Self::BuiltIn(code))
            }
            // Plain string → user code
            serde_json::Value::String(s) => Ok(Self::User(s.clone())),
            _ => Err(serde::de::Error::custom(
                "error_code must be a string or {\"__builtin_task_code__\": \"...\"}",
            )),
        }
    }
}

impl From<BuiltInTaskCode> for TaskErrorCode {
    fn from(code: BuiltInTaskCode) -> Self {
        Self::BuiltIn(code)
    }
}
impl From<OperationalErrorCode> for TaskErrorCode {
    fn from(code: OperationalErrorCode) -> Self {
        Self::BuiltIn(code.into())
    }
}
impl From<ContractCode> for TaskErrorCode {
    fn from(code: ContractCode) -> Self {
        Self::BuiltIn(code.into())
    }
}
impl From<RetrievalCode> for TaskErrorCode {
    fn from(code: RetrievalCode) -> Self {
        Self::BuiltIn(code.into())
    }
}
impl From<OutcomeCode> for TaskErrorCode {
    fn from(code: OutcomeCode) -> Self {
        Self::BuiltIn(code.into())
    }
}
impl From<String> for TaskErrorCode {
    fn from(code: String) -> Self {
        Self::User(code)
    }
}
impl From<&str> for TaskErrorCode {
    fn from(code: &str) -> Self {
        Self::User(code.to_owned())
    }
}

// =============================================================================
// TaskError
// =============================================================================

/// Error payload for a task result.
///
/// Produced by:
/// - Task functions returning an error result
/// - Library runtime failures (execution, serialization, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskError {
    /// Error code identifying the failure category.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<TaskErrorCode>,

    /// Human-readable error message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Serialized error cause data (for cross-process error propagation).
    /// Named `cause` in Rust (Python uses `exception` — Rust has no exceptions).
    /// Carries structured metadata about the original error (e.g. type name,
    /// traceback equivalent). Preserved for retry matching and diagnostics.
    #[serde(rename = "exception", skip_serializing_if = "Option::is_none")]
    pub cause: Option<serde_json::Value>,

    /// Arbitrary error data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl TaskError {
    /// Create a TaskError with a built-in error code and message.
    pub fn builtin(code: impl Into<BuiltInTaskCode>, message: impl Into<String>) -> Self {
        Self {
            error_code: Some(TaskErrorCode::BuiltIn(code.into())),
            message: Some(message.into()),
            cause: None,
            data: None,
        }
    }

    /// Create a TaskError with a user-defined error code and message.
    pub fn user(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_code: Some(TaskErrorCode::User(code.into())),
            message: Some(message.into()),
            cause: None,
            data: None,
        }
    }
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.error_code, &self.message) {
            (Some(code), Some(msg)) => write!(f, "[{}] {}", code, msg),
            (Some(code), None) => write!(f, "[{}]", code),
            (None, Some(msg)) => write!(f, "{}", msg),
            (None, None) => write!(f, "TaskError"),
        }
    }
}

impl std::error::Error for TaskError {}

/// Error from a failed sub-workflow, extending TaskError with child workflow details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubWorkflowError {
    /// Base task error fields.
    #[serde(flatten)]
    pub task_error: TaskError,

    /// ID of the failed child workflow.
    pub sub_workflow_id: String,

    /// Summary of the child workflow's execution.
    pub sub_workflow_summary: SubWorkflowSummary,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_code_tagged_serde() {
        let code = BuiltInTaskCode::Operational(OperationalErrorCode::BrokerError);
        let json = serde_json::to_string(&code).unwrap();
        assert_eq!(json, r#"{"__builtin_task_code__":"BROKER_ERROR"}"#);
        let back: BuiltInTaskCode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, code);
    }

    #[test]
    fn task_error_code_builtin_serde() {
        let code = TaskErrorCode::BuiltIn(OperationalErrorCode::WorkerCrashed.into());
        let json = serde_json::to_string(&code).unwrap();
        assert!(json.contains("__builtin_task_code__"));
        assert!(json.contains("WORKER_CRASHED"));
        let back: TaskErrorCode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, code);
    }

    #[test]
    fn task_error_code_user_serde() {
        let code = TaskErrorCode::User("MY_ERROR".to_owned());
        let json = serde_json::to_string(&code).unwrap();
        assert_eq!(json, "\"MY_ERROR\"");
        let back: TaskErrorCode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, code);
    }

    #[test]
    fn task_error_builtin_constructor() {
        let err = TaskError::builtin(RetrievalCode::WaitTimeout, "timed out");
        match &err.error_code {
            Some(TaskErrorCode::BuiltIn(BuiltInTaskCode::Retrieval(
                RetrievalCode::WaitTimeout,
            ))) => {}
            other => panic!("expected WaitTimeout, got {:?}", other),
        }
    }

    #[test]
    fn task_error_user_constructor() {
        let err = TaskError::user("TOO_LARGE", "payload exceeds 10MB");
        assert_eq!(
            err.error_code,
            Some(TaskErrorCode::User("TOO_LARGE".to_owned()))
        );
    }

    #[test]
    fn task_error_full_serde_round_trip() {
        let err = TaskError {
            error_code: Some(TaskErrorCode::from(RetrievalCode::WaitTimeout)),
            message: Some("timeout".to_owned()),
            cause: None,
            data: Some(serde_json::json!({"elapsed_ms": 5000})),
        };
        let json = serde_json::to_string(&err).unwrap();
        let back: TaskError = serde_json::from_str(&json).unwrap();
        assert_eq!(back.error_code, err.error_code);
        assert_eq!(back.message, err.message);
        assert_eq!(back.data, err.data);
    }

    #[test]
    fn all_operational_codes_round_trip() {
        let codes = [
            OperationalErrorCode::UnhandledError,
            OperationalErrorCode::TaskError,
            OperationalErrorCode::WorkerCrashed,
            OperationalErrorCode::BrokerError,
            OperationalErrorCode::WorkerResolutionError,
            OperationalErrorCode::WorkerSerializationError,
            OperationalErrorCode::ResultDeserializationError,
            OperationalErrorCode::WorkflowEnqueueFailed,
            OperationalErrorCode::SubworkflowLoadFailed,
        ];
        for code in codes {
            let builtin = BuiltInTaskCode::Operational(code.clone());
            let json = serde_json::to_string(&builtin).unwrap();
            let back: BuiltInTaskCode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, builtin);
        }
    }

    #[test]
    fn all_outcome_codes_round_trip() {
        let codes = [
            OutcomeCode::TaskCancelled,
            OutcomeCode::WorkflowPaused,
            OutcomeCode::WorkflowFailed,
            OutcomeCode::WorkflowCancelled,
            OutcomeCode::UpstreamSkipped,
            OutcomeCode::SubworkflowFailed,
            OutcomeCode::WorkflowSuccessCaseNotMet,
            OutcomeCode::WorkflowStopped,
            OutcomeCode::SendSuppressed,
        ];
        for code in codes {
            let builtin = BuiltInTaskCode::Outcome(code.clone());
            let json = serde_json::to_string(&builtin).unwrap();
            let back: BuiltInTaskCode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, builtin);
        }
    }

    #[test]
    fn builtin_from_str_known() {
        assert!(BuiltInTaskCode::parse("BROKER_ERROR").is_some());
        assert!(BuiltInTaskCode::parse("WAIT_TIMEOUT").is_some());
        assert!(BuiltInTaskCode::parse("TASK_CANCELLED").is_some());
        assert!(BuiltInTaskCode::parse("ARGUMENT_TYPE_MISMATCH").is_some());
        assert!(BuiltInTaskCode::parse("RETURN_TYPE_MISMATCH").is_some());
    }

    #[test]
    fn builtin_from_str_unknown() {
        assert!(BuiltInTaskCode::parse("NOT_A_CODE").is_none());
        assert!(BuiltInTaskCode::parse("my_user_code").is_none());
    }

    #[test]
    fn display_codes() {
        assert_eq!(
            BuiltInTaskCode::Operational(OperationalErrorCode::BrokerError).to_string(),
            "BROKER_ERROR",
        );
        assert_eq!(
            TaskErrorCode::User("CUSTOM".to_owned()).to_string(),
            "CUSTOM",
        );
    }

    #[test]
    fn sub_workflow_error_serde_round_trip() {
        let err = SubWorkflowError {
            task_error: TaskError {
                error_code: Some(TaskErrorCode::from(OperationalErrorCode::UnhandledError)),
                message: Some("child workflow failed".to_owned()),
                cause: None,
                data: None,
            },
            sub_workflow_id: "wf-child-789".to_owned(),
            sub_workflow_summary: SubWorkflowSummary {
                status: "FAILED".to_owned(),
                is_success: false,
                success_case: None,
                output: None,
                total_tasks: 4,
                completed_tasks: 2,
                failed_tasks: 1,
                skipped_tasks: 1,
                error_summary: Some("task 'transform' exploded".to_owned()),
                child_workflow_id: "wf-child-789".to_owned(),
            },
        };

        let json = serde_json::to_string(&err).unwrap();
        let back: SubWorkflowError = serde_json::from_str(&json).unwrap();
        assert_eq!(back.sub_workflow_id, "wf-child-789");
        assert!(!back.sub_workflow_summary.is_success);
    }

    #[test]
    fn sub_workflow_error_flattened_json_structure() {
        let err = SubWorkflowError {
            task_error: TaskError {
                error_code: Some(TaskErrorCode::from(OperationalErrorCode::BrokerError)),
                message: Some("connection lost".to_owned()),
                cause: None,
                data: None,
            },
            sub_workflow_id: "wf-abc".to_owned(),
            sub_workflow_summary: SubWorkflowSummary {
                status: "FAILED".to_owned(),
                is_success: false,
                success_case: None,
                output: None,
                total_tasks: 1,
                completed_tasks: 0,
                failed_tasks: 1,
                skipped_tasks: 0,
                error_summary: None,
                child_workflow_id: "wf-abc".to_owned(),
            },
        };

        let json_value = serde_json::to_value(&err).unwrap();
        assert!(json_value.get("error_code").is_some());
        assert!(json_value.get("message").is_some());
        assert!(json_value.get("sub_workflow_id").is_some());
        assert!(
            json_value.get("task_error").is_none(),
            "task_error should be flattened, not nested"
        );
    }
}

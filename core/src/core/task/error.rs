use serde::de::{DeserializeOwned, Error as _};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

use crate::catalog::CodeEnum;
use crate::core::codec::user_value::{
    check_user_value_text, UserValueRefused, MAX_VALUE_JSON_DEPTH,
};
use crate::core::codec::{check_json_text, object_members, JsonText, StrictJsonError};
use crate::core::workflow::summary::SubWorkflowSummary;

// =============================================================================
// Built-in error code families (matches Python's 4-family structure)
// =============================================================================

/// Something actually broke in execution.
///
/// Typical caller behavior: log, retry conditionally, inspect infra/runtime failure.
///
/// Members, wire values and order are Python's (`clients/python/horsies/core/models/tasks.py`), plus
/// `WORKFLOW_START_INCOMPLETE`, which both catalogs hold.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, horsies_core_derive::CodeCatalog,
)]
#[code_catalog(module = "task_codes", base = "str_enum")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationalErrorCode {
    /// The task panicked or raised an error that no exception mapper turned into a task error.
    UnhandledException,
    /// The worker caught an exception outside the task wrapper, or a validator raised on a default.
    TaskException,
    WorkerCrashed,
    BrokerError,
    WorkerResolutionError,
    WorkerSerializationError,
    ResultDeserializationError,
    WorkflowEnqueueFailed,
    SubworkflowLoadFailed,
    /// Workflow recovery failed with a persistent error in `workflow_recovery_failure_limit` reaper
    /// passes, so the workflow failed closed.
    WorkflowRecoveryLimitReached,
    /// A child workflow would pass the depth limit (32), so it did not start and its sub-workflow node failed.
    SubworkflowDepthLimitReached,
    /// This task held the event loop of its shared async child past the block limit, before its
    /// `timeout_ms`. The worker killed the child; a retry (only when `auto_retry_for` names this code)
    /// runs in an isolated child.
    TaskBlockedLoop,
    /// Another task held the event loop of this task's shared async child past the block limit, before
    /// this task's `timeout_ms`. The worker killed the child. A retry runs only when `auto_retry_for`
    /// names this code.
    NeighbourBlockedLoop,
    /// A RUNNING workflow has no node rows: its start did not finish. Recovery fails it with this code.
    WorkflowStartIncomplete,
}

impl OperationalErrorCode {
    pub const ALL: [Self; 14] = [
        Self::UnhandledException,
        Self::TaskException,
        Self::WorkerCrashed,
        Self::BrokerError,
        Self::WorkerResolutionError,
        Self::WorkerSerializationError,
        Self::ResultDeserializationError,
        Self::WorkflowEnqueueFailed,
        Self::SubworkflowLoadFailed,
        Self::WorkflowRecoveryLimitReached,
        Self::SubworkflowDepthLimitReached,
        Self::TaskBlockedLoop,
        Self::NeighbourBlockedLoop,
        Self::WorkflowStartIncomplete,
    ];
}

/// A code/typing/usage contract was violated.
///
/// Typical caller behavior: fix code or type assumptions, do not blindly retry.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, horsies_core_derive::CodeCatalog,
)]
#[code_catalog(module = "task_codes", base = "str_enum")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContractCode {
    ReturnTypeMismatch,
    WorkflowCtxMissingId,
    /// A handle-level read (`TaskHandle.get`, `WorkflowHandle.get`) asked for a typed value, but the ok
    /// type cannot be obtained, for example on a handle rebuilt by id without the task or workflow
    /// registered in this process. The result-returning form is `BrokerErrorCode.NO_TYPE_AVAILABLE`.
    NoTypeAvailable,
}

impl ContractCode {
    pub const ALL: [Self; 3] = [
        Self::ReturnTypeMismatch,
        Self::WorkflowCtxMissingId,
        Self::NoTypeAvailable,
    ];
}

/// The caller cannot get the value right now or cannot locate it.
///
/// Typical caller behavior: wait, poll again, handle missing value as normal flow.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, horsies_core_derive::CodeCatalog,
)]
#[code_catalog(module = "task_codes", base = "str_enum")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RetrievalCode {
    WaitTimeout,
    TaskNotFound,
    WorkflowNotFound,
    ResultNotAvailable,
    ResultNotReady,
    /// A sync get was called on a thread that runs an event loop, so this call site cannot wait for
    /// the value. Never cached: the async form, or a sync call from a thread without a running loop,
    /// reads it.
    #[code_catalog(blocking_form_only)]
    AsyncContext,
    /// An argument of the get call was refused before anything ran, for
    /// example a wait limit above 2^32 - 1 ms. Never cached: a call with a
    /// valid argument reads the value.
    InvalidInput,
    /// A result wait was called in a process that runs task bodies, so it returns at once and waits
    /// for nothing. Never cached: a task composes with other tasks through a workflow, not through a
    /// wait.
    WaitInsideTask,
}

impl RetrievalCode {
    pub const ALL: [Self; 8] = [
        Self::WaitTimeout,
        Self::TaskNotFound,
        Self::WorkflowNotFound,
        Self::ResultNotAvailable,
        Self::ResultNotReady,
        Self::AsyncContext,
        Self::InvalidInput,
        Self::WaitInsideTask,
    ];
}

/// A non-bug execution outcome or control-flow result.
///
/// Typical caller behavior: branch on outcome, continue control flow.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, horsies_core_derive::CodeCatalog,
)]
#[code_catalog(module = "task_codes", base = "str_enum")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OutcomeCode {
    TaskCancelled,
    TaskExpired,
    TaskTimeout,
    WorkflowPaused,
    WorkflowExpired,
    WorkflowFailed,
    WorkflowCancelled,
    UpstreamSkipped,
    SubworkflowFailed,
    WorkflowSuccessCaseNotMet,
    /// A COMPLETED workflow has no output: its output node (or a terminal node of an outputless workflow) is
    /// SKIPPED.
    WorkflowOutputSkipped,
}

impl OutcomeCode {
    pub const ALL: [Self; 11] = [
        Self::TaskCancelled,
        Self::TaskExpired,
        Self::TaskTimeout,
        Self::WorkflowPaused,
        Self::WorkflowExpired,
        Self::WorkflowFailed,
        Self::WorkflowCancelled,
        Self::UpstreamSkipped,
        Self::SubworkflowFailed,
        Self::WorkflowSuccessCaseNotMet,
        Self::WorkflowOutputSkipped,
    ];
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
    /// The stored text of the code (for example `"BROKER_ERROR"`): the wire value of its catalog member.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Operational(code) => code.wire_value(),
            Self::Contract(code) => code.wire_value(),
            Self::Retrieval(code) => code.wire_value(),
            Self::Outcome(code) => code.wire_value(),
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
        f.write_str(self.as_str())
    }
}

/// Serializes as `{"__builtin_task_code__": "BROKER_ERROR"}`.
impl Serialize for BuiltInTaskCode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("__builtin_task_code__", self.as_str())?;
        map.end()
    }
}

/// Deserializes from `{"__builtin_task_code__": "BROKER_ERROR"}`, exactly that one key (Python
/// `_scan_task_error_code`, `core/codec/typed.py:1253-1302`).
impl<'de> Deserialize<'de> for BuiltInTaskCode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
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
// Custom codes
// =============================================================================

/// Code texts that are reserved and are not task codes. `WORKFLOW_STOPPED` names a run that stopped before the
/// task body ran; a task cannot return it, so no finalize path can take a returned error for a stop.
pub const RESERVED_NON_TASK_CODES: [&str; 1] = ["WORKFLOW_STOPPED"];

// The text rule of an `auto_retry_for` code. `CustomCode::parse` does not apply it.
mod code_text;

pub use code_text::{check_error_code_text, CodeTextRefusal};

// The codes that a task declares and the `error_code` metric label.
mod declared;

pub use declared::{
    error_code_label, DeclaredCodeRefusal, DeclaredCodesRefused, DeclaredErrorCode,
    DeclaredErrorCodes, ErrorCodeLabel, UNDECLARED_LABEL,
};

/// An error code that task code chose.
///
/// Only [`CustomCode::parse`] builds one, so a custom code never has the name of a built-in code or of a reserved
/// code (Python `TaskError._validate_code`, `core/models/tasks.py:278-311`). A built-in code is carried
/// only by [`TaskErrorCode::BuiltIn`], so the `error_code` column, `auto_retry_for` and the monitoring
/// families see one meaning for each code text.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CustomCode(String);

/// Why a text cannot be a custom error code.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CustomCodeRefused {
    /// The text is the name of a built-in code. Python: "is a reserved built-in error code"
    /// (`core/models/tasks.py:304-311`).
    #[error("'{0}' is a reserved built-in error code; pass the built-in code instead")]
    BuiltInName(BuiltInTaskCode),
    /// The text is a reserved name that is not a task code ([`RESERVED_NON_TASK_CODES`]).
    #[error("'{0}' is a reserved error code name and is not a task code")]
    ReservedName(&'static str),
}

impl CustomCode {
    /// Build a custom code from its text. Refuses the name of a built-in code and a reserved name.
    pub fn parse(text: impl Into<String>) -> Result<Self, CustomCodeRefused> {
        let text = text.into();
        if let Some(builtin) = BuiltInTaskCode::parse(&text) {
            return Err(CustomCodeRefused::BuiltInName(builtin));
        }
        match RESERVED_NON_TASK_CODES
            .into_iter()
            .find(|reserved| *reserved == text)
        {
            Some(reserved) => Err(CustomCodeRefused::ReservedName(reserved)),
            None => Ok(Self(text)),
        }
    }

    /// The code text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for CustomCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// =============================================================================
// TaskErrorCode — either built-in or custom
// =============================================================================

/// Error code discriminator: either a built-in code (4 families) or a custom code.
///
/// Serialization (Python `TaskError._serialize_code`, `core/models/tasks.py:313-327`):
/// - Built-in: `{"__builtin_task_code__": "BROKER_ERROR"}`
/// - Custom: `"MY_CUSTOM_ERROR"`
///
/// A read refuses `null`, a plain text that names a built-in or reserved code, an unknown built-in name, and a
/// tag object with any other key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskErrorCode {
    BuiltIn(BuiltInTaskCode),
    Custom(CustomCode),
}

impl std::fmt::Display for TaskErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BuiltIn(code) => write!(f, "{}", code),
            Self::Custom(code) => write!(f, "{}", code),
        }
    }
}

impl Serialize for TaskErrorCode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::BuiltIn(code) => code.serialize(serializer),
            Self::Custom(code) => serializer.serialize_str(code.as_str()),
        }
    }
}

impl<'de> Deserialize<'de> for TaskErrorCode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::String(text) => CustomCode::parse(text)
                .map(Self::Custom)
                .map_err(D::Error::custom),
            serde_json::Value::Object(tag) => {
                serde_json::from_value::<BuiltInTaskCode>(serde_json::Value::Object(tag))
                    .map(Self::BuiltIn)
                    .map_err(D::Error::custom)
            }
            serde_json::Value::Null => Err(D::Error::custom("error_code must not be null")),
            serde_json::Value::Bool(_)
            | serde_json::Value::Number(_)
            | serde_json::Value::Array(_) => Err(D::Error::custom(
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
impl From<CustomCode> for TaskErrorCode {
    fn from(code: CustomCode) -> Self {
        Self::Custom(code)
    }
}

// =============================================================================
// The exception record
// =============================================================================

/// The stored form of an exception: Python's `FlattenedException`, five text keys
/// (`core/models/tasks.py:177-191`, written by `flatten_exception`, `core/codec/error_payload.py:96-118`).
/// A read refuses a missing key and any other key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExceptionRecord {
    /// The class name.
    #[serde(rename = "type")]
    pub type_name: String,
    /// The module of the class.
    pub module: String,
    /// The text of the exception.
    pub message: String,
    /// The repr of the exception.
    pub repr: String,
    /// The printed traceback.
    pub traceback: String,
}

// =============================================================================
// TaskError
// =============================================================================

/// The key of the replacement `data` of an error whose `data` cannot be stored (Python `reportable_error`,
/// `core/codec/typed.py:927`).
pub const DATA_REFUSED_KEY: &str = "data_refused";

const EXCEPTION_KEY: &str = "exception";
const ERROR_CODE_KEY: &str = "error_code";
const DATA_KEY: &str = "data";
const MESSAGE_KEY: &str = "message";
const SUB_WORKFLOW_ID_KEY: &str = "sub_workflow_id";
const SUB_WORKFLOW_SUMMARY_KEY: &str = "sub_workflow_summary";

/// The deepest `data` that an error stores. Python reads the whole error object as one typed value of at most
/// `MAX_TYPED_JSON_DEPTH` levels (`decode_task_error`, `core/codec/typed.py:632-680`), and the object is one of
/// those levels.
pub const ERROR_DATA_DEPTH_LIMIT: usize = MAX_VALUE_JSON_DEPTH - 1;

/// The deepest `output` that the summary of a sub-workflow error stores: the error object and the summary
/// object are two of the levels.
pub const SUMMARY_OUTPUT_DEPTH_LIMIT: usize = MAX_VALUE_JSON_DEPTH - 2;

/// Which class a stored error has (Python: `TaskError` or its subclass `SubWorkflowError`,
/// `core/models/tasks.py:352-367`). The stored object of a sub-workflow error has the two fields of
/// [`SubWorkflowFailure`] at its top level, next to the four keys of every error.
#[derive(Debug, Clone, PartialEq)]
pub enum TaskErrorKind {
    /// A plain `TaskError`.
    Plain,
    /// A failed child workflow (`SubWorkflowError`).
    SubWorkflow(Box<SubWorkflowFailure>),
}

/// The child workflow of a sub-workflow error.
#[derive(Debug, Clone, PartialEq)]
pub struct SubWorkflowFailure {
    /// ID of the failed child workflow.
    pub sub_workflow_id: uuid::Uuid,
    /// Summary of the child workflow's execution.
    pub sub_workflow_summary: SubWorkflowSummary,
}

impl SubWorkflowFailure {
    fn summary_without_output(&self) -> SubWorkflowSummary {
        SubWorkflowSummary {
            output: None,
            ..self.sub_workflow_summary.clone()
        }
    }
}

/// Error payload for a task result (Python `TaskError`, `core/models/tasks.py:214-350`).
///
/// Produced by task functions returning an error result, and by library runtime failures (execution,
/// serialization, etc.).
///
/// Stored form (Python `encode_task_error`, `core/codec/typed.py:1053-1075`): an object with the four keys
/// `exception`, `error_code`, `data` and `message` in this order, nulls kept, and for a sub-workflow error
/// `sub_workflow_id` and `sub_workflow_summary` after them. When the `data` (or the output of the summary)
/// breaks the rules of a user value (`crate::core::codec::user_value`), the error is stored with its code and
/// message kept and `data` = `{"data_refused": "<cause>"}`; a refused summary output is stored as `null`
/// (Python `reportable_error`, `core/codec/typed.py:885-934`). A read refuses an object with a missing or
/// `null` code, an unknown key, an exception record of another shape, or a `data` that breaks the rules.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskError {
    /// The flattened exception, when the library recorded one.
    pub exception: Option<Box<ExceptionRecord>>,

    /// Error code identifying the failure category. Required: the retry rule and outcome routing read it.
    pub error_code: TaskErrorCode,

    /// Error data, as the text it was written or stored with: an integer outside the `i64` and `u64` range
    /// keeps every digit.
    pub data: Option<JsonText>,

    /// Human-readable error message.
    pub message: Option<String>,

    /// The class of the error.
    pub kind: TaskErrorKind,
}

/// Why a stored error object does not read as a [`TaskError`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoredErrorRefused {
    /// The stored text breaks the strict read rule.
    #[error("a stored task error is not strict JSON: {0}")]
    NotJson(StrictJsonError),
    /// The stored error is not a JSON object.
    #[error("a stored task error must be a JSON object")]
    NotAnObject,
    /// The object has a key that the error class does not declare.
    #[error("unexpected key {0:?} in a stored task error")]
    UnknownKey(String),
    /// A required key is missing.
    #[error("a stored task error has no {0}")]
    MissingField(&'static str),
    /// A field does not read as its type.
    #[error("field {field} of a stored task error does not parse: {cause}")]
    FieldInvalid { field: &'static str, cause: String },
    /// A field breaks the rules of a user value.
    #[error("field {field} of a stored task error is refused: {refusal}")]
    UserValue {
        field: &'static str,
        refusal: UserValueRefused,
    },
}

impl TaskError {
    /// Create a TaskError with a built-in error code and message.
    pub fn built_in(code: impl Into<BuiltInTaskCode>, message: impl Into<String>) -> Self {
        Self {
            exception: None,
            error_code: TaskErrorCode::BuiltIn(code.into()),
            data: None,
            message: Some(message.into()),
            kind: TaskErrorKind::Plain,
        }
    }

    /// Create a TaskError with a custom error code and message.
    ///
    /// Refuses a code text that names a built-in code or a reserved code (Python refuses it at construction,
    /// `core/models/tasks.py:304-311`).
    pub fn custom(
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Result<Self, CustomCodeRefused> {
        Ok(Self {
            exception: None,
            error_code: TaskErrorCode::Custom(CustomCode::parse(code)?),
            data: None,
            message: Some(message.into()),
            kind: TaskErrorKind::Plain,
        })
    }

    /// The child workflow of a sub-workflow failure, or `None` for a plain error.
    pub fn sub_workflow_details(&self) -> Option<&SubWorkflowFailure> {
        match &self.kind {
            TaskErrorKind::Plain => None,
            TaskErrorKind::SubWorkflow(failure) => Some(failure),
        }
    }

    /// Why this error is not stored as it is, or `None`. The `data` is checked first, then the output of the
    /// sub-workflow summary. Pure.
    pub fn storage_refusal(&self) -> Option<UserValueRefused> {
        self.data_refusal()
            .or_else(|| self.summary_output_refusal())
    }

    fn data_refusal(&self) -> Option<UserValueRefused> {
        self.data
            .as_ref()
            .and_then(|data| check_user_value_text(data, ERROR_DATA_DEPTH_LIMIT).err())
    }

    fn summary_output_refusal(&self) -> Option<UserValueRefused> {
        match &self.kind {
            TaskErrorKind::Plain => None,
            TaskErrorKind::SubWorkflow(failure) => failure
                .sub_workflow_summary
                .output
                .as_ref()
                .and_then(|output| check_user_value_text(output, SUMMARY_OUTPUT_DEPTH_LIMIT).err()),
        }
    }

    /// Read a stored error object from its text (Python `loads_json`, then `decode_task_error`,
    /// `core/codec/typed.py:1078-1130`). The `data` and the output of a sub-workflow summary keep their stored
    /// text: no user value is read into a `serde_json::Value`.
    ///
    /// The object is a sub-workflow error when it has `sub_workflow_id`; then both sub-workflow fields are
    /// required. `exception`, `data` and `message` may be missing or `null`. Pure.
    pub fn from_stored_text(text: &str) -> Result<Self, StoredErrorRefused> {
        check_json_text(text).map_err(StoredErrorRefused::NotJson)?;
        // The text is strict JSON, so it does not read as members only when it is not an object.
        let members =
            object_members(text).map_err(|_not_an_object| StoredErrorRefused::NotAnObject)?;
        let is_sub_workflow = members
            .iter()
            .any(|(key, _value)| key.as_str() == SUB_WORKFLOW_ID_KEY);
        if let Some((key, _value)) = members
            .iter()
            .find(|(key, _value)| !is_stored_error_key(key, is_sub_workflow))
        {
            return Err(StoredErrorRefused::UnknownKey(key.clone()));
        }
        let error_code: TaskErrorCode = required_field(&members, ERROR_CODE_KEY)?;
        let exception: Option<Box<ExceptionRecord>> = optional_field(&members, EXCEPTION_KEY)?;
        let message: Option<String> = optional_field(&members, MESSAGE_KEY)?;
        let data = stored_data(&members)?;
        let kind = match is_sub_workflow {
            false => TaskErrorKind::Plain,
            true => TaskErrorKind::SubWorkflow(Box::new(read_sub_workflow_failure(&members)?)),
        };
        Ok(Self {
            exception,
            error_code,
            data,
            message,
            kind,
        })
    }
}

#[cfg(test)]
impl TaskError {
    /// The `data` decoded as a JSON value, for a test that compares it with `serde_json::json!`.
    pub(crate) fn data_value(&self) -> Option<serde_json::Value> {
        self.data.as_ref().map(|data| {
            data.decode()
                .expect("the data of a task error decodes as a JSON value")
        })
    }
}

fn is_stored_error_key(key: &str, is_sub_workflow: bool) -> bool {
    match key {
        EXCEPTION_KEY | ERROR_CODE_KEY | DATA_KEY | MESSAGE_KEY => true,
        SUB_WORKFLOW_ID_KEY | SUB_WORKFLOW_SUMMARY_KEY => is_sub_workflow,
        _ => false,
    }
}

/// The text of the member `key` of a stored error, or `None` when it has no such member. Pure.
fn member<'a>(members: &[(String, &'a RawValue)], key: &str) -> Option<&'a RawValue> {
    members
        .iter()
        .find(|(name, _value)| name.as_str() == key)
        .map(|(_name, value)| *value)
}

/// A field that a stored error must have, decoded from its text. Pure.
fn required_field<T: DeserializeOwned>(
    members: &[(String, &RawValue)],
    field: &'static str,
) -> Result<T, StoredErrorRefused> {
    let text = member(members, field).ok_or(StoredErrorRefused::MissingField(field))?;
    decode_field(text, field)
}

/// A field that a stored error may leave out or set to `null`, decoded from its text. Pure.
fn optional_field<T: DeserializeOwned>(
    members: &[(String, &RawValue)],
    field: &'static str,
) -> Result<Option<T>, StoredErrorRefused> {
    match member(members, field) {
        None => Ok(None),
        Some(text) => decode_field::<Option<T>>(text, field),
    }
}

/// The value of one field from its text. Pure.
fn decode_field<T: DeserializeOwned>(
    text: &RawValue,
    field: &'static str,
) -> Result<T, StoredErrorRefused> {
    serde_json::from_str(text.get()).map_err(|error| StoredErrorRefused::FieldInvalid {
        field,
        cause: error.to_string(),
    })
}

/// The `data` of a stored error as its stored text, `None` when it is missing or `null`. It follows the rules of
/// a user value with [`ERROR_DATA_DEPTH_LIMIT`] levels. Pure.
fn stored_data(members: &[(String, &RawValue)]) -> Result<Option<JsonText>, StoredErrorRefused> {
    let data = match member(members, DATA_KEY) {
        None => return Ok(None),
        Some(text) if text.get() == "null" => return Ok(None),
        Some(text) => JsonText::of_checked_part(text),
    };
    check_user_value_text(&data, ERROR_DATA_DEPTH_LIMIT).map_err(|refusal| {
        StoredErrorRefused::UserValue {
            field: DATA_KEY,
            refusal,
        }
    })?;
    Ok(Some(data))
}

fn read_sub_workflow_failure(
    members: &[(String, &RawValue)],
) -> Result<SubWorkflowFailure, StoredErrorRefused> {
    let sub_workflow_id: uuid::Uuid = required_field(members, SUB_WORKFLOW_ID_KEY)?;
    let sub_workflow_summary: SubWorkflowSummary =
        required_field(members, SUB_WORKFLOW_SUMMARY_KEY)?;
    if let Some(output) = &sub_workflow_summary.output {
        check_user_value_text(output, SUMMARY_OUTPUT_DEPTH_LIMIT).map_err(|refusal| {
            StoredErrorRefused::UserValue {
                field: SUB_WORKFLOW_SUMMARY_KEY,
                refusal,
            }
        })?;
    }
    Ok(SubWorkflowFailure {
        sub_workflow_id,
        sub_workflow_summary,
    })
}

/// The `data` slot of a stored error.
enum StoredData<'a> {
    /// The error's own `data`, or `null`.
    Kept(Option<&'a JsonText>),
    /// `{"data_refused": <cause>}`.
    Refused(&'static str),
}

impl Serialize for StoredData<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Kept(data) => data.serialize(serializer),
            Self::Refused(cause) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry(DATA_REFUSED_KEY, cause)?;
                map.end()
            }
        }
    }
}

/// Writes the stored form (see [`TaskError`]). Never fails over `data`: a refused `data` is replaced, so the
/// code and the message are always stored.
impl Serialize for TaskError {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let data = match self.storage_refusal() {
            Some(refusal) => StoredData::Refused(refusal.stored_cause()),
            None => StoredData::Kept(self.data.as_ref()),
        };
        let key_count = match &self.kind {
            TaskErrorKind::Plain => 4,
            TaskErrorKind::SubWorkflow(_) => 6,
        };
        let mut map = serializer.serialize_map(Some(key_count))?;
        map.serialize_entry(EXCEPTION_KEY, &self.exception)?;
        map.serialize_entry(ERROR_CODE_KEY, &self.error_code)?;
        map.serialize_entry(DATA_KEY, &data)?;
        map.serialize_entry(MESSAGE_KEY, &self.message)?;
        if let TaskErrorKind::SubWorkflow(failure) = &self.kind {
            map.serialize_entry(SUB_WORKFLOW_ID_KEY, &failure.sub_workflow_id)?;
            match self.summary_output_refusal() {
                None => {
                    map.serialize_entry(SUB_WORKFLOW_SUMMARY_KEY, &failure.sub_workflow_summary)?;
                }
                Some(_) => map
                    .serialize_entry(SUB_WORKFLOW_SUMMARY_KEY, &failure.summary_without_output())?,
            }
        }
        map.end()
    }
}

/// Reads the stored form through [`TaskError::from_stored_text`]. The object is taken as raw text and read by
/// the core's strict reader, so its depth bound is the core's, not the caller's deserializer's, and the `data`
/// keeps its text.
impl<'de> Deserialize<'de> for TaskError {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        Self::from_stored_text(raw.get()).map_err(D::Error::custom)
    }
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.message {
            Some(message) => write!(f, "[{}] {}", self.error_code, message),
            None => write!(f, "[{}]", self.error_code),
        }
    }
}

impl std::error::Error for TaskError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::codec::JsonText;
    use crate::core::task::result::TaskResult;
    use crate::core::workflow::status::WorkflowStatus;
    use serde_json::json;

    fn custom_code(text: &str) -> TaskErrorCode {
        TaskErrorCode::Custom(CustomCode::parse(text).unwrap())
    }

    /// The text of a JSON value, as the `data` of an error.
    fn text(value: &serde_json::Value) -> JsonText {
        JsonText::of(value).unwrap()
    }

    fn failed_summary() -> SubWorkflowSummary {
        SubWorkflowSummary {
            status: WorkflowStatus::Failed,
            output: None,
            total_tasks: 3,
            completed_tasks: 1,
            failed_tasks: 1,
            skipped_tasks: 1,
            error_summary: Some("child task 'b' failed".to_owned()),
        }
    }

    fn sub_workflow_error(summary: SubWorkflowSummary) -> TaskError {
        TaskError {
            exception: None,
            error_code: OutcomeCode::SubworkflowFailed.into(),
            data: None,
            message: Some("Subworkflow 00000000-0000-0000-0000-000000000000 failed".to_owned()),
            kind: TaskErrorKind::SubWorkflow(Box::new(SubWorkflowFailure {
                sub_workflow_id: uuid::Uuid::nil(),
                sub_workflow_summary: summary,
            })),
        }
    }

    /// A list with `depth` levels and a number inside.
    fn nested_list(depth: usize) -> serde_json::Value {
        let mut value = json!([1]);
        for _ in 1..depth {
            value = serde_json::Value::Array(vec![value]);
        }
        value
    }

    #[test]
    fn builtin_code_tagged_serde() {
        let code = BuiltInTaskCode::Operational(OperationalErrorCode::BrokerError);
        let json = serde_json::to_string(&code).unwrap();
        assert_eq!(json, r#"{"__builtin_task_code__":"BROKER_ERROR"}"#);
        let back: BuiltInTaskCode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, code);
    }

    /// Python `_scan_task_error_code`: the tag object has exactly one key.
    #[test]
    fn a_built_in_tag_with_another_key_is_refused() {
        for text in [
            r#"{"__builtin_task_code__":"BROKER_ERROR","other":1}"#,
            r#"{"__builtin_task_code__":"BROKER_ERROR","__h_x":1}"#,
            r#"{"__builtin_task_code__":"NOT_A_CODE"}"#,
            r#"{"code":"BROKER_ERROR"}"#,
        ] {
            assert!(
                serde_json::from_str::<TaskErrorCode>(text).is_err(),
                "{text}"
            );
        }
    }

    #[test]
    fn task_error_code_builtin_serde() {
        let code = TaskErrorCode::BuiltIn(OperationalErrorCode::WorkerCrashed.into());
        let json = serde_json::to_string(&code).unwrap();
        assert_eq!(json, r#"{"__builtin_task_code__":"WORKER_CRASHED"}"#);
        let back: TaskErrorCode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, code);
    }

    #[test]
    fn task_error_code_custom_serde() {
        let code = custom_code("MY_ERROR");
        let json = serde_json::to_string(&code).unwrap();
        assert_eq!(json, "\"MY_ERROR\"");
        let back: TaskErrorCode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, code);
    }

    /// Python `clients/python/tests/unit/test_error_code_families.py:200-216`: a custom code with a built-in name is refused at
    /// construction, also when the text is built at run time.
    #[test]
    fn a_custom_code_with_a_built_in_name_is_refused() {
        let dynamic = format!("{}_{}", "WORKFLOW", "PAUSED");
        for text in [
            "WORKFLOW_PAUSED",
            "BROKER_ERROR",
            "RETURN_TYPE_MISMATCH",
            "RESULT_NOT_READY",
            "TASK_TIMEOUT",
            dynamic.as_str(),
        ] {
            let builtin = BuiltInTaskCode::parse(text).unwrap();
            assert_eq!(
                CustomCode::parse(text),
                Err(CustomCodeRefused::BuiltInName(builtin.clone()))
            );
            assert_eq!(
                TaskError::custom(text, "m").unwrap_err(),
                CustomCodeRefused::BuiltInName(builtin)
            );
            assert!(CustomCode::parse(text)
                .unwrap_err()
                .to_string()
                .contains("reserved built-in error code"));
        }
    }

    #[test]
    fn workflow_stopped_is_reserved_and_not_a_task_code() {
        assert_eq!(BuiltInTaskCode::parse("WORKFLOW_STOPPED"), None);
        assert_eq!(
            CustomCode::parse("WORKFLOW_STOPPED"),
            Err(CustomCodeRefused::ReservedName("WORKFLOW_STOPPED"))
        );
        assert!(serde_json::from_str::<TaskErrorCode>(r#""WORKFLOW_STOPPED""#).is_err());
        assert!(serde_json::from_str::<TaskErrorCode>(
            r#"{"__builtin_task_code__":"WORKFLOW_STOPPED"}"#
        )
        .is_err());
    }

    /// Python decode refuses a plain text with a built-in name and a null code.
    #[test]
    fn a_read_refuses_a_plain_built_in_name_and_null() {
        assert!(serde_json::from_str::<TaskErrorCode>(r#""TASK_TIMEOUT""#).is_err());
        assert!(serde_json::from_str::<TaskErrorCode>("null").is_err());
        assert!(serde_json::from_str::<TaskErrorCode>("7").is_err());
    }

    #[test]
    fn task_error_built_in_constructor() {
        let err = TaskError::built_in(RetrievalCode::WaitTimeout, "timed out");
        assert_eq!(
            err.error_code,
            TaskErrorCode::BuiltIn(BuiltInTaskCode::Retrieval(RetrievalCode::WaitTimeout))
        );
        assert_eq!(err.kind, TaskErrorKind::Plain);
    }

    #[test]
    fn task_error_custom_constructor() {
        let err = TaskError::custom("TOO_LARGE", "payload exceeds 10MB").unwrap();
        assert_eq!(err.error_code, custom_code("TOO_LARGE"));
    }

    /// Python stores all four keys, nulls kept, in the field order of the model
    /// (`core/codec/typed.py:1053-1075`).
    #[test]
    fn the_stored_text_has_the_four_keys_with_nulls() {
        let err = TaskError::custom("RATE_LIMITED", "slow down").unwrap();
        assert_eq!(
            serde_json::to_string(&err).unwrap(),
            r#"{"exception":null,"error_code":"RATE_LIMITED","data":null,"message":"slow down"}"#
        );
        let builtin = TaskError {
            data: Some(text(&json!({"elapsed_ms": 5000}))),
            message: None,
            ..TaskError::built_in(RetrievalCode::WaitTimeout, "")
        };
        assert_eq!(
            serde_json::to_string(&builtin).unwrap(),
            r#"{"exception":null,"error_code":{"__builtin_task_code__":"WAIT_TIMEOUT"},"data":{"elapsed_ms":5000},"message":null}"#
        );
    }

    #[test]
    fn task_error_full_serde_round_trip() {
        let err = TaskError {
            exception: Some(Box::new(ExceptionRecord {
                type_name: "ValueError".to_owned(),
                module: "builtins".to_owned(),
                message: "boom".to_owned(),
                repr: "ValueError('boom')".to_owned(),
                traceback: "Traceback (most recent call last):\n".to_owned(),
            })),
            error_code: RetrievalCode::WaitTimeout.into(),
            data: Some(text(&json!({"elapsed_ms": 5000}))),
            message: Some("timeout".to_owned()),
            kind: TaskErrorKind::Plain,
        };
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.starts_with(
            r#"{"exception":{"type":"ValueError","module":"builtins","message":"boom","repr":"ValueError('boom')","traceback":"#
        ));
        let back: TaskError = serde_json::from_str(&json).unwrap();
        assert_eq!(back, err);
    }

    /// Python's `exception`, `data` and `message` default to null; only `error_code` is required.
    #[test]
    fn a_read_takes_missing_optional_keys_as_null() {
        let err: TaskError = serde_json::from_str(r#"{"error_code":"X"}"#).unwrap();
        assert_eq!(err.error_code, custom_code("X"));
        assert_eq!(err.exception, None);
        assert_eq!(err.data, None);
        assert_eq!(err.message, None);
        assert_eq!(err.kind, TaskErrorKind::Plain);
    }

    /// Python `TaskError` (`extra='forbid'`, required `error_code`) refuses these objects.
    #[test]
    fn a_read_refuses_a_missing_code_and_unknown_keys() {
        for (text, refusal) in [
            (r#"{}"#, StoredErrorRefused::MissingField(ERROR_CODE_KEY)),
            (
                r#"{"message":"m"}"#,
                StoredErrorRefused::MissingField(ERROR_CODE_KEY),
            ),
            (
                r#"{"error_code":"X","extra":1}"#,
                StoredErrorRefused::UnknownKey("extra".to_owned()),
            ),
            (
                r#"{"error_code":"X","sub_workflow_summary":null}"#,
                StoredErrorRefused::UnknownKey(SUB_WORKFLOW_SUMMARY_KEY.to_owned()),
            ),
            (
                r#"{"error_code":"X","sub_workflow_id":"00000000-0000-0000-0000-000000000000"}"#,
                StoredErrorRefused::MissingField(SUB_WORKFLOW_SUMMARY_KEY),
            ),
        ] {
            assert_eq!(TaskError::from_stored_text(text), Err(refusal), "{text}");
        }
        for text in [
            r#"{"error_code":null}"#,
            r#"[]"#,
            r#"{"error_code":"X","exception":{"type":"E"}}"#,
            r#"{"error_code":"X","exception":{"type":"E","module":"m","message":"t","repr":"r","traceback":"","extra":1}}"#,
            r#"{"error_code":"X","exception":"text"}"#,
            r#"{"error_code":"X","message":7}"#,
        ] {
            assert!(serde_json::from_str::<TaskError>(text).is_err(), "{text}");
        }
    }

    /// Python `clients/python/tests/unit/test_strict_codec.py:650-680`: a `data` that cannot be stored keeps the code and the
    /// message; `data` becomes `{"data_refused": <cause>}`.
    #[test]
    fn a_refused_data_keeps_the_code_and_the_message() {
        for (data, cause) in [
            (json!({"__h_x": 1}), "StrictJsonError"),
            (
                json!({"a": [{"__builtin_task_code__": "X"}]}),
                "StrictJsonError",
            ),
            (json!({"__horsies_workflow_ctx__": {}}), "StrictJsonError"),
            (nested_list(MAX_VALUE_JSON_DEPTH), "ValueValidationError"),
            (nested_list(250), "ValueValidationError"),
        ] {
            let err = TaskError {
                data: Some(text(&data)),
                ..TaskError::custom("RATE_LIMITED", "m").unwrap()
            };
            let text = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(err)).unwrap();
            let stored: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(stored["err"]["data"], json!({ DATA_REFUSED_KEY: cause }));
            let read = serde_json::from_str::<TaskResult<serde_json::Value>>(&text)
                .unwrap()
                .unwrap_err();
            assert_eq!(read.error_code, custom_code("RATE_LIMITED"));
            assert_eq!(read.message.as_deref(), Some("m"));
        }
    }

    /// The error object is one of the `MAX_VALUE_JSON_DEPTH` levels, so `data` keeps 199.
    #[test]
    fn data_of_199_levels_is_kept() {
        let data = nested_list(ERROR_DATA_DEPTH_LIMIT);
        let err = TaskError {
            data: Some(text(&data)),
            ..TaskError::built_in(OperationalErrorCode::BrokerError, "m")
        };
        assert_eq!(err.storage_refusal(), None);
        let back: TaskError = serde_json::from_str(&serde_json::to_string(&err).unwrap()).unwrap();
        assert_eq!(back.data, Some(text(&data)));
    }

    #[test]
    fn good_data_is_unchanged() {
        for data in [
            json!({"a": 1.5}),
            json!("x"),
            json!([1, "y", null]),
            json!(5),
        ] {
            let err = TaskError {
                data: Some(text(&data)),
                ..TaskError::custom("RATE_LIMITED", "m").unwrap()
            };
            let back: TaskError =
                serde_json::from_str(&serde_json::to_string(&err).unwrap()).unwrap();
            assert_eq!(back.data, Some(text(&data)));
        }
    }

    /// A stored `data` outside the `i64` and `u64` range keeps its stored text through the read and the write,
    /// and decodes as `u128` or `i128`.
    #[test]
    fn data_outside_i64_and_u64_keeps_its_stored_text() {
        for data in [
            "340282366920938463463374607431768211455",
            r#"{"below": -9223372036854775809}"#,
        ] {
            let stored = format!(
                r#"{{"exception":null,"error_code":"RATE_LIMITED","data":{data},"message":"m"}}"#
            );
            let read = TaskError::from_stored_text(&stored).unwrap();
            assert_eq!(read.data.as_ref().map(JsonText::get), Some(data));
            assert_eq!(serde_json::to_string(&read).unwrap(), stored);
            let via_serde: TaskError = serde_json::from_str(&stored).unwrap();
            assert_eq!(via_serde, read);
        }
        let wide = TaskError::from_stored_text(
            r#"{"error_code":"X","data":340282366920938463463374607431768211455}"#,
        )
        .unwrap();
        assert_eq!(wide.data.unwrap().decode::<u128>().unwrap(), u128::MAX);
        let written = TaskError {
            data: Some(JsonText::of(&(i128::from(i64::MIN) - 1)).unwrap()),
            ..TaskError::custom("RATE_LIMITED", "m").unwrap()
        };
        let envelope =
            crate::core::codec::task_result_to_json_string(&TaskResult::<()>::Err(written))
                .unwrap();
        assert!(
            envelope.contains(r#""data":-9223372036854775809,"#),
            "{envelope}"
        );
    }

    /// Python decodes `data` with the reserved-key scan: a stored error with an engine key in `data` does not
    /// read.
    #[test]
    fn a_read_refuses_data_with_an_engine_key() {
        let text = r#"{"exception":null,"error_code":"X","data":{"__h_task_result__":true},"message":null}"#;
        assert!(serde_json::from_str::<TaskError>(text).is_err());
    }

    /// Row 35 of the workflow ledger: `sub_workflow_id` and `sub_workflow_summary` are top-level fields of the
    /// stored error, after the four keys (Python `SubWorkflowError`, `core/models/tasks.py:352-367`).
    #[test]
    fn a_sub_workflow_error_stores_its_fields_at_the_top_level() {
        let err = sub_workflow_error(failed_summary());
        let text = serde_json::to_string(&err).unwrap();
        assert_eq!(
            text,
            concat!(
                r#"{"exception":null,"error_code":{"__builtin_task_code__":"SUBWORKFLOW_FAILED"},"data":null,"#,
                r#""message":"Subworkflow 00000000-0000-0000-0000-000000000000 failed","#,
                r#""sub_workflow_id":"00000000-0000-0000-0000-000000000000","#,
                r#""sub_workflow_summary":{"status":"FAILED","output":null,"total_tasks":3,"completed_tasks":1,"#,
                r#""failed_tasks":1,"skipped_tasks":1,"error_summary":"child task 'b' failed"}}"#,
            )
        );
        let back: TaskError = serde_json::from_str(&text).unwrap();
        assert_eq!(back, err);
        let details = back.sub_workflow_details().expect("a sub-workflow error");
        assert_eq!(details.sub_workflow_id, uuid::Uuid::nil());
        assert_eq!(details.sub_workflow_summary.failed_tasks, 1);
    }

    #[test]
    fn sub_workflow_details_survive_the_result_envelope() {
        let err = sub_workflow_error(failed_summary());
        let json = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(err)).unwrap();
        let decoded: TaskResult<serde_json::Value> = serde_json::from_str(&json).unwrap();
        let recovered = decoded.unwrap_err();
        let details = recovered.sub_workflow_details().unwrap();
        assert_eq!(details.sub_workflow_summary.completed_tasks, 1);
    }

    /// A summary output outside the `i64` and `u64` range is written with its text.
    #[test]
    fn a_summary_output_is_written_as_its_text() {
        for output in [
            "340282366920938463463374607431768211455",
            "-9223372036854775809",
        ] {
            let summary = SubWorkflowSummary {
                output: Some(JsonText::parse(output).unwrap()),
                ..failed_summary()
            };
            let text = serde_json::to_string(&sub_workflow_error(summary)).unwrap();
            assert!(text.contains(&format!(r#""output":{output},"#)), "{text}");
        }
    }

    #[test]
    fn sub_workflow_details_none_for_plain_error() {
        let err = TaskError::custom("SOME_OTHER_ERROR", "nope").unwrap();
        assert!(err.sub_workflow_details().is_none());
        let bare = TaskError::built_in(OutcomeCode::SubworkflowFailed, "no child");
        assert!(bare.sub_workflow_details().is_none());
    }

    /// Python `_repaired_subworkflow_error`: a summary output that cannot be stored is stored as null, the class
    /// and the code are kept, and `data` names the cause.
    #[test]
    fn a_refused_summary_output_is_stored_as_null() {
        let summary = SubWorkflowSummary {
            output: Some(JsonText::parse(r#"{"__h_task_result__": true}"#).unwrap()),
            ..failed_summary()
        };
        let err = sub_workflow_error(summary);
        let stored: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&err).unwrap()).unwrap();
        assert_eq!(
            stored["sub_workflow_summary"]["output"],
            serde_json::Value::Null
        );
        assert_eq!(
            stored["data"],
            json!({ DATA_REFUSED_KEY: "StrictJsonError" })
        );
        let back = TaskError::from_stored_text(&stored.to_string()).unwrap();
        assert_eq!(
            back.error_code,
            TaskErrorCode::from(OutcomeCode::SubworkflowFailed)
        );
        assert!(back.sub_workflow_details().is_some());
    }

    #[test]
    fn all_operational_codes_round_trip() {
        for code in OperationalErrorCode::ALL {
            let builtin = BuiltInTaskCode::Operational(code);
            let json = serde_json::to_string(&builtin).unwrap();
            let back: BuiltInTaskCode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, builtin);
        }
    }

    /// Python `OperationalErrorCode` names the two codes of a blocked event
    /// loop this way.
    #[test]
    fn the_blocked_loop_codes_have_python_names() {
        assert_eq!(
            serde_json::to_string(&OperationalErrorCode::TaskBlockedLoop).unwrap(),
            "\"TASK_BLOCKED_LOOP\""
        );
        assert_eq!(
            serde_json::to_string(&OperationalErrorCode::NeighbourBlockedLoop).unwrap(),
            "\"NEIGHBOUR_BLOCKED_LOOP\""
        );
    }

    #[test]
    fn all_outcome_codes_round_trip() {
        for code in OutcomeCode::ALL {
            let builtin = BuiltInTaskCode::Outcome(code);
            let json = serde_json::to_string(&builtin).unwrap();
            let back: BuiltInTaskCode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, builtin);
        }
    }

    #[test]
    fn task_timeout_code_string_value() {
        assert_eq!(
            BuiltInTaskCode::Outcome(OutcomeCode::TaskTimeout).as_str(),
            "TASK_TIMEOUT"
        );
        assert_eq!(
            BuiltInTaskCode::parse("TASK_TIMEOUT"),
            Some(BuiltInTaskCode::Outcome(OutcomeCode::TaskTimeout))
        );
    }

    #[test]
    fn builtin_from_str_known() {
        assert!(BuiltInTaskCode::parse("BROKER_ERROR").is_some());
        assert!(BuiltInTaskCode::parse("WAIT_TIMEOUT").is_some());
        assert!(BuiltInTaskCode::parse("TASK_CANCELLED").is_some());
        assert!(BuiltInTaskCode::parse("NO_TYPE_AVAILABLE").is_some());
        assert!(BuiltInTaskCode::parse("WAIT_INSIDE_TASK").is_some());
        assert!(BuiltInTaskCode::parse("NEIGHBOUR_BLOCKED_LOOP").is_some());
        assert!(BuiltInTaskCode::parse("RETURN_TYPE_MISMATCH").is_some());
    }

    #[test]
    fn builtin_from_str_unknown() {
        assert!(BuiltInTaskCode::parse("NOT_A_CODE").is_none());
        assert!(BuiltInTaskCode::parse("my_user_code").is_none());
    }

    #[test]
    fn names_that_python_does_not_have_are_not_built_in() {
        // The core's former names and members; Python's catalogs
        // (`clients/python/horsies/core/models/tasks.py`) hold none of them.
        for text in [
            "UNHANDLED_ERROR",
            "TASK_ERROR",
            "ARGUMENT_TYPE_MISMATCH",
            "SEND_SUPPRESSED",
        ] {
            assert!(BuiltInTaskCode::parse(text).is_none(), "{text}");
            assert!(CustomCode::parse(text).is_ok(), "{text}");
        }
    }

    #[test]
    fn python_s_added_members_are_built_in_names() {
        for text in [
            "UNHANDLED_EXCEPTION",
            "TASK_EXCEPTION",
            "TASK_BLOCKED_LOOP",
            "NEIGHBOUR_BLOCKED_LOOP",
            "NO_TYPE_AVAILABLE",
            "ASYNC_CONTEXT",
            "WAIT_INSIDE_TASK",
        ] {
            assert!(BuiltInTaskCode::parse(text).is_some(), "{text}");
            assert!(
                matches!(
                    CustomCode::parse(text),
                    Err(CustomCodeRefused::BuiltInName(_))
                ),
                "{text}"
            );
        }
    }

    #[test]
    fn display_codes_and_errors() {
        assert_eq!(
            BuiltInTaskCode::Operational(OperationalErrorCode::BrokerError).to_string(),
            "BROKER_ERROR",
        );
        assert_eq!(custom_code("CUSTOM").to_string(), "CUSTOM");
        assert_eq!(
            TaskError::custom("CUSTOM", "went wrong")
                .unwrap()
                .to_string(),
            "[CUSTOM] went wrong"
        );
        let no_message = TaskError {
            message: None,
            ..TaskError::built_in(OperationalErrorCode::BrokerError, "")
        };
        assert_eq!(no_message.to_string(), "[BROKER_ERROR]");
    }
}

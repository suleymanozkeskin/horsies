//! The send operation: one enqueue of one task through the core.
//!
//! The Python client builds one frozen `EnqueueRequest` per send attempt: a
//! resend of the same task id passes the same values with the send kind
//! `REPEAT`, so a task that finished answers from history and does not run
//! again. The core's refusals that need no connection run first, then the
//! first send of a handle installs the schema once, as Python's broker does.
//! The outcome is `Enqueued`, `SendValidationFailed` (the core refused the
//! input, the options, the idempotency key or an empty retention class key,
//! before any connect), `SendEnqueueFailed` for a retention class key that
//! is not empty and that no class derives (as Python's broker answers it),
//! or one class per other variant of the core's send error catalog
//! (`TaskSendError`), each with only the facts of its state.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use horsies_core::core::task::send_types::send_error_from_enqueue;
use horsies_core::{
    BrokerError, BrokerErrorCode, CheckedSend, EnqueueRefusal, EnqueueRefusalCause,
    EnqueueRetention, FailureCause, RetentionClassKeyRefused, SendKind, SendPart, TaskSendError,
    TaskSendPayload,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use super::arguments::{utc_instant, RerunInputPolicy, StartArgument, StartRule};
use crate::core_failures::FailureException;
use crate::handle::BrokerConnection;

/// One send, as the core takes it. Built once, at the boundary.
#[derive(Clone, Debug)]
pub struct EnqueueCommand {
    task_id: Uuid,
    task_name: String,
    queue_name: String,
    priority: i32,
    input_json: String,
    sent_at: DateTime<Utc>,
    start: StartRule,
    good_until: Option<DateTime<Utc>>,
    task_options_json: Option<String>,
    idempotency_key: Option<String>,
    retention_class_key: String,
    rerun_input: RerunInputPolicy,
    send_kind: SendKind,
}

impl EnqueueCommand {
    /// The send payload of the core's error catalog, for the error path.
    fn payload(&self, retention: EnqueueRetention) -> TaskSendPayload {
        let enqueue_delay_seconds = match self.start {
            StartRule::AfterDelay { seconds } => Some(seconds),
            StartRule::Now | StartRule::At(_) => None,
        };
        TaskSendPayload {
            task_name: self.task_name.clone(),
            queue_name: self.queue_name.clone(),
            priority: self.priority,
            input_json: self.input_json.clone(),
            sent_at: self.sent_at,
            good_until: self.good_until,
            enqueue_delay_seconds,
            task_options: self.task_options_json.clone(),
            idempotency_key: self.idempotency_key.clone(),
            retention,
        }
    }
}

/// A frozen send request. `None` for `good_until`, `task_options_json` or
/// `idempotency_key` means no deadline, no options, no key.
/// `retention_class_key` is the stored class key (`forever`, or
/// `standard_<days>d`); the core checks it at the send. `send_kind` is
/// `FIRST` for the first send of the task id and `REPEAT` for a send retry;
/// another text raises `ValueError`.
#[pyclass(module = "horsies._core", frozen)]
pub struct EnqueueRequest {
    command: Arc<EnqueueCommand>,
}

impl EnqueueRequest {
    pub fn command(&self) -> Arc<EnqueueCommand> {
        Arc::clone(&self.command)
    }
}

#[pymethods]
impl EnqueueRequest {
    #[new]
    #[pyo3(signature = (
        *,
        task_id,
        task_name,
        queue_name,
        priority,
        input_json,
        sent_at,
        start,
        good_until,
        task_options_json,
        idempotency_key,
        retention_class_key,
        rerun_input,
        send_kind
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        task_id: Uuid,
        task_name: String,
        queue_name: String,
        priority: i32,
        input_json: String,
        sent_at: &Bound<'_, PyAny>,
        start: StartArgument<'_>,
        good_until: Option<Bound<'_, PyAny>>,
        task_options_json: Option<String>,
        idempotency_key: Option<String>,
        retention_class_key: String,
        rerun_input: &str,
        send_kind: &str,
    ) -> PyResult<Self> {
        let good_until = match good_until {
            Some(instant) => Some(utc_instant(&instant)?),
            None => None,
        };
        let command = EnqueueCommand {
            task_id,
            task_name,
            queue_name,
            priority,
            input_json,
            sent_at: utc_instant(sent_at)?,
            start: start.rule(),
            good_until,
            task_options_json,
            idempotency_key,
            retention_class_key,
            rerun_input: RerunInputPolicy::parse(rerun_input)?,
            send_kind: send_kind_of(send_kind).map_err(PyValueError::new_err)?,
        };
        Ok(Self {
            command: Arc::new(command),
        })
    }

    /// The task id of this send.
    #[getter]
    fn task_id(&self) -> Uuid {
        self.command.task_id
    }
}

/// The send kind of a request: `FIRST` or `REPEAT`. Pure.
fn send_kind_of(text: &str) -> Result<SendKind, String> {
    match text {
        "FIRST" => Ok(SendKind::First),
        "REPEAT" => Ok(SendKind::Repeat),
        other => Err(format!(
            "send_kind must be 'FIRST' or 'REPEAT', not {other:?}"
        )),
    }
}

/// The task is stored, or it already was (the same command under the same
/// task id or idempotency key).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct Enqueued {
    pub task_id: Uuid,
}

/// Sends are suppressed. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SendSuppressed {
    pub message: String,
}

/// A blocking send ran on a thread that runs an event loop or a tokio
/// runtime (Python `TaskSendAsyncContext`). Nothing was written. A client
/// produces it before the send, never the core's enqueue.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SendAsyncContext {
    pub task_id: Uuid,
    pub message: String,
}

/// The cause of a refused send: the refused text does not read as JSON.
/// `reader_error` is the JSON reader's error.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SendRefusalUnreadableJson {
    pub reader_error: String,
}

/// The cause of a refused send: a rule refused a value that reads. The
/// message is the only fact.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct SendRefusalByRule {}

/// The cause of a refused send: a rule of the version-1 command or of the
/// scoped idempotency key refused a value. `rule` is the rule's text.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SendRefusalByValueRule {
    pub rule: String,
}

/// The cause of a refused send.
#[derive(Clone, Debug)]
pub enum RefusalCauseRecord {
    UnreadableJson(SendRefusalUnreadableJson),
    ByRule(SendRefusalByRule),
    ByValueRule(SendRefusalByValueRule),
}

impl<'py> IntoPyObject<'py> for RefusalCauseRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::UnreadableJson(unreadable) => unreadable.into_bound_py_any(py),
            Self::ByRule(by_rule) => by_rule.into_bound_py_any(py),
            Self::ByValueRule(by_value_rule) => by_value_rule.into_bound_py_any(py),
        }
    }
}

/// The core refused the send before any statement. Nothing was written.
/// `code` is `INVALID_INPUT` (the input, or a fact of the call: the start,
/// the sent instant, the idempotency key, the fingerprint) or
/// `INVALID_TASK_OPTIONS`. `cause` is `SendRefusalUnreadableJson`,
/// `SendRefusalByRule` or `SendRefusalByValueRule`.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct SendValidationFailed {
    #[pyo3(get)]
    pub code: String,
    #[pyo3(get)]
    pub message: String,
    cause: RefusalCauseRecord,
}

#[pymethods]
impl SendValidationFailed {
    /// `SendRefusalUnreadableJson`, `SendRefusalByRule` or
    /// `SendRefusalByValueRule`.
    #[getter]
    fn cause<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.cause.clone().into_bound_py_any(py)
    }
}

impl SendValidationFailed {
    /// The record of a core refusal of a send of `task_name`. Pure.
    ///
    /// A rule of the version-1 command or of the scoped idempotency key
    /// keeps Python's message (`enqueue input refused for <task>: <rule>`,
    /// `idempotency key refused for <task>: <rule>`) and the rule as the
    /// cause. Any other refusal keeps `core_message`, the core's text of the
    /// error.
    fn of(task_name: &str, refusal: &EnqueueRefusal, core_message: String) -> Self {
        let (message, cause) = match &refusal.cause {
            EnqueueRefusalCause::JsonUnreadable(reader_error) => (
                core_message,
                RefusalCauseRecord::UnreadableJson(SendRefusalUnreadableJson {
                    reader_error: reader_error.clone(),
                }),
            ),
            EnqueueRefusalCause::RuleRefused => (
                core_message,
                RefusalCauseRecord::ByRule(SendRefusalByRule {}),
            ),
            EnqueueRefusalCause::CommandRuleRefused(rule) => (
                format!("enqueue input refused for {task_name}: {rule}"),
                RefusalCauseRecord::ByValueRule(SendRefusalByValueRule { rule: rule.clone() }),
            ),
            EnqueueRefusalCause::KeyRuleRefused(rule) => (
                format!("idempotency key refused for {task_name}: {rule}"),
                RefusalCauseRecord::ByValueRule(SendRefusalByValueRule { rule: rule.clone() }),
            ),
        };
        Self {
            code: refused_code(refusal.part).to_string(),
            message,
            cause,
        }
    }
}

/// The broker code of a refused part. Pure.
fn refused_code(part: SendPart) -> BrokerErrorCode {
    match part {
        SendPart::Input => BrokerErrorCode::InvalidInput,
        SendPart::TaskOptions => BrokerErrorCode::InvalidTaskOptions,
    }
}

/// The enqueue failed; the row may or may not exist. A retention class key
/// that no class derives is this record too, built before any statement:
/// nothing was written, and `retryable` is false
/// (`retention_key_refused`). `exception` is the cause, with the subject
/// `enqueue`.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SendEnqueueFailed {
    pub task_id: Uuid,
    pub message: String,
    pub retryable: bool,
    pub exception: FailureException,
}

/// The subject of the exception of a failed enqueue.
const ENQUEUE_SUBJECT: &str = "enqueue";

/// The task id exists with another command or idempotency key. Nothing was
/// written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SendPayloadMismatch {
    pub task_id: Uuid,
    pub message: String,
}

/// The input is larger than the configured limit. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SendPayloadTooLarge {
    pub message: String,
    pub size_bytes: u64,
    pub limit_bytes: u64,
}

/// The idempotency key is reserved by `owner_task_id` with another command.
/// Nothing was written; a resend after the reservation ends is accepted.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SendKeyConflict {
    pub task_id: Uuid,
    pub owner_task_id: Uuid,
    pub message: String,
}

/// The outcome of one send.
pub enum SendOutcome {
    Enqueued(Uuid),
    /// The core refused the input or the options before any statement.
    Invalid(SendValidationFailed),
    /// No retention class derives the class key, which is not empty;
    /// nothing was written.
    RetentionKeyRefused(SendEnqueueFailed),
    /// The send catalog's answer to a broker error; it carries the cause of
    /// that error.
    Refused(TaskSendError),
}

impl<'py> IntoPyObject<'py> for SendOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Enqueued(task_id) => Enqueued { task_id }.into_bound_py_any(py),
            Self::Invalid(invalid) => invalid.into_bound_py_any(py),
            Self::RetentionKeyRefused(failed) => failed.into_bound_py_any(py),
            Self::Refused(error) => refusal_record(py, error),
        }
    }
}

fn refusal_record(py: Python<'_>, error: TaskSendError) -> PyResult<Bound<'_, PyAny>> {
    match error {
        TaskSendError::SendSuppressed { message } => {
            SendSuppressed { message }.into_bound_py_any(py)
        }
        TaskSendError::AsyncContext {
            task_id, message, ..
        } => SendAsyncContext { task_id, message }.into_bound_py_any(py),
        // The send catalog's refusals besides the core's own (`refused`
        // takes those): a reservation window or a value that does not
        // serialize. Both refuse a fact of the call, which the input carries.
        TaskSendError::ValidationFailed { message, .. } => SendValidationFailed {
            code: BrokerErrorCode::InvalidInput.to_string(),
            message,
            cause: RefusalCauseRecord::ByRule(SendRefusalByRule {}),
        }
        .into_bound_py_any(py),
        TaskSendError::EnqueueFailed {
            task_id,
            message,
            retryable,
            cause,
            ..
        } => SendEnqueueFailed {
            task_id,
            exception: FailureException::new(ENQUEUE_SUBJECT, message.clone(), retryable, cause),
            message,
            retryable,
        }
        .into_bound_py_any(py),
        TaskSendError::PayloadMismatch {
            task_id, message, ..
        } => SendPayloadMismatch { task_id, message }.into_bound_py_any(py),
        TaskSendError::PayloadTooLarge {
            message,
            size_bytes,
            limit_bytes,
        } => SendPayloadTooLarge {
            message,
            size_bytes,
            limit_bytes,
        }
        .into_bound_py_any(py),
        TaskSendError::IdempotencyKeyConflict {
            task_id,
            message,
            owner_task_id,
            ..
        } => SendKeyConflict {
            task_id,
            owner_task_id,
            message,
        }
        .into_bound_py_any(py),
    }
}

/// Enqueue one task. The core's refusals that need no connection run first
/// (`CheckedSend::check`), as `_prepare_enqueue` ran before the schema
/// install (`horsies/core/brokers/postgres.py` at `v0.7.0`): a refused send
/// answers `SendValidationFailed` with no connect and no statement. Then the
/// schema is installed once per handle; a failed connect or install is
/// `SendEnqueueFailed`, as Python's enqueue
/// reports it. Writes; a caller that leaves early sees the outcome as
/// unknown, and a resend of the same request with the send kind `REPEAT` is
/// safe.
pub async fn enqueue_operation(
    connection: BrokerConnection,
    command: Arc<EnqueueCommand>,
) -> SendOutcome {
    let retention = match send_retention(&command.retention_class_key) {
        Ok(retention) => retention,
        Err(refused) => {
            return retention_refused(command.task_id, &command.task_name, refused);
        }
    };
    let (enqueued_at, enqueue_delay_seconds) = match command.start {
        StartRule::Now => (None, None),
        StartRule::AfterDelay { seconds } => (None, Some(seconds)),
        StartRule::At(at) => (Some(at), None),
    };
    let retain_rerun_input = command
        .rerun_input
        .keep()
        .unwrap_or_else(|| connection.retain_rerun_input_default());
    let send = match CheckedSend::check(
        &command.task_name,
        &command.input_json,
        &command.queue_name,
        command.priority,
        Some(command.sent_at),
        enqueued_at,
        command.good_until,
        command.task_options_json.as_deref(),
        Some(command.task_id),
        enqueue_delay_seconds,
        command.idempotency_key.as_deref(),
        retention,
        retain_rerun_input,
    ) {
        Ok(send) => send,
        Err(error) => return refused(&command, retention, &error),
    };
    let broker = match connection.initialized_broker().await {
        Ok(broker) => broker,
        Err(error) => return refused(&command, retention, &error),
    };
    match broker.enqueue_checked(&send, command.send_kind).await {
        Ok(task_id) => SendOutcome::Enqueued(task_id),
        Err(error) => refused(&command, retention, &error),
    }
}

/// Why the retention class key of a send was refused.
#[derive(Debug)]
enum RetentionKeyRefusal {
    /// The key is empty: a rule of the version-1 command.
    Empty,
    /// The key is not empty, and no class derives it.
    NotDerived(RetentionClassKeyRefused),
}

/// The retention of the class key `class_key` of a send. Pure.
///
/// Errors: `RetentionKeyRefusal::Empty` for an empty key, and
/// `RetentionKeyRefusal::NotDerived` for a key that no class derives.
fn send_retention(class_key: &str) -> Result<EnqueueRetention, RetentionKeyRefusal> {
    match class_key.is_empty() {
        true => Err(RetentionKeyRefusal::Empty),
        false => {
            EnqueueRetention::from_class_key(class_key).map_err(RetentionKeyRefusal::NotDerived)
        }
    }
}

/// The outcome of a refused retention class key of the send `task_id` of
/// `task_name`, as Python's broker answers it. Nothing was written. Pure.
///
/// - An empty key: `EnqueueCommandV1` refuses it before any statement:
///   `SendValidationFailed` (`INVALID_INPUT`, with the rule as the cause).
/// - A key that no class derives: `retention_key_refused` (`ENQUEUE_FAILED`,
///   not retryable).
fn retention_refused(task_id: Uuid, task_name: &str, refused: RetentionKeyRefusal) -> SendOutcome {
    match refused {
        RetentionKeyRefusal::Empty => {
            let refusal = EnqueueRefusal::empty_retention_class_key();
            let core_message = BrokerError::EnqueueContract(refusal.clone()).to_string();
            SendOutcome::Invalid(SendValidationFailed::of(task_name, &refusal, core_message))
        }
        RetentionKeyRefusal::NotDerived(refused_key) => SendOutcome::RetentionKeyRefused(
            retention_key_refused(task_id, task_name, &refused_key),
        ),
    }
}

/// The record of a retention class key that no class derives, as Python's
/// broker answers it: Python sends the key, the CHECK
/// `horsies_tasks_retention_class_key_form` refuses the insert, and the
/// enqueue is `BrokerEnqueueFailed` with that cause, not retryable. The core
/// refuses the key before any statement; nothing is written in both. The
/// message has Python's form, with the refusal as the cause. Pure.
fn retention_key_refused(
    task_id: Uuid,
    task_name: &str,
    refused: &RetentionClassKeyRefused,
) -> SendEnqueueFailed {
    let message = format!("Failed to enqueue task {task_name}: {refused}");
    SendEnqueueFailed {
        task_id,
        exception: FailureException::new(
            ENQUEUE_SUBJECT,
            message.clone(),
            false,
            FailureCause::PreconditionFailed,
        ),
        message,
        retryable: false,
    }
}

/// The outcome of a failed enqueue: a core refusal keeps its part and its
/// cause; every other error goes through the core's send catalog rule
/// (`send_error_from_enqueue`). Pure.
fn refused(
    command: &EnqueueCommand,
    retention: EnqueueRetention,
    error: &BrokerError,
) -> SendOutcome {
    match error {
        BrokerError::EnqueueContract(refusal) => SendOutcome::Invalid(SendValidationFailed::of(
            &command.task_name,
            refusal,
            error.to_string(),
        )),
        other => SendOutcome::Refused(send_error_from_enqueue(
            command.task_id,
            command.payload(retention),
            other,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_send_kinds_are_read_and_any_other_text_is_refused() {
        assert_eq!(send_kind_of("FIRST"), Ok(SendKind::First));
        assert_eq!(send_kind_of("REPEAT"), Ok(SendKind::Repeat));
        let refused = send_kind_of("repeat").expect_err("send kind texts are upper case");
        assert!(refused.contains("\"repeat\""), "{refused}");
    }

    #[test]
    fn an_input_refusal_is_invalid_input_with_the_reader_error() {
        let refusal = EnqueueRefusal {
            part: SendPart::Input,
            message: "task input refused: trailing characters".to_owned(),
            cause: EnqueueRefusalCause::JsonUnreadable("trailing characters".to_owned()),
        };
        let record = SendValidationFailed::of("t", &refusal, refusal.message.clone());
        assert_eq!(record.code, "INVALID_INPUT");
        assert_eq!(record.message, refusal.message);
        match record.cause {
            RefusalCauseRecord::UnreadableJson(unreadable) => {
                assert_eq!(unreadable.reader_error, "trailing characters");
            }
            other => panic!("an unreadable input gave the cause {other:?}"),
        }
    }

    #[test]
    fn a_retention_key_that_no_class_derives_is_a_failed_enqueue_that_is_not_retried() {
        let task_id = Uuid::from_u128(7);
        let refused = EnqueueRetention::from_class_key("standard_0d")
            .expect_err("a period of 0 days is below the bounds");
        let record = retention_key_refused(task_id, "send.task", &refused);
        assert_eq!(record.task_id, task_id);
        assert!(!record.retryable);
        assert!(
            record.message.starts_with(
                "Failed to enqueue task send.task: retention class key \"standard_0d\""
            ),
            "{}",
            record.message
        );
    }

    #[test]
    fn an_options_refusal_by_a_rule_is_invalid_task_options_with_no_cause() {
        let refusal = EnqueueRefusal::by_rule(
            SendPart::TaskOptions,
            "task_options refused for t: retry_policy is not an object".to_owned(),
        );
        let record = SendValidationFailed::of("t", &refusal, refusal.message.clone());
        assert_eq!(record.code, "INVALID_TASK_OPTIONS");
        assert_eq!(record.message, refusal.message);
        assert!(
            matches!(record.cause, RefusalCauseRecord::ByRule(_)),
            "{:?}",
            record.cause
        );
    }

    /// Python's broker refuses a value of the version-1 command with
    /// `enqueue input refused for <task>: <rule>` and the `ValueError` of
    /// `EnqueueCommandV1`, and a key with `idempotency key refused for
    /// <task>: <rule>` and the `ValueError` of `ScopedIdempotencyKey`.
    #[test]
    fn a_command_rule_and_a_key_rule_keep_the_python_message_and_the_rule() {
        for (cause, message, rule) in [
            (
                EnqueueRefusalCause::CommandRuleRefused(
                    "priority must be between 1 and 100".to_owned(),
                ),
                "enqueue input refused for send.task: priority must be between 1 and 100",
                "priority must be between 1 and 100",
            ),
            (
                EnqueueRefusalCause::KeyRuleRefused("idempotency key must be non-empty".to_owned()),
                "idempotency key refused for send.task: idempotency key must be non-empty",
                "idempotency key must be non-empty",
            ),
        ] {
            let refusal = EnqueueRefusal {
                part: SendPart::Input,
                message: "the core's text".to_owned(),
                cause,
            };
            let record =
                SendValidationFailed::of("send.task", &refusal, "the core's text".to_owned());
            assert_eq!(record.code, "INVALID_INPUT", "{rule}");
            assert_eq!(record.message, message);
            match record.cause {
                RefusalCauseRecord::ByValueRule(by_value_rule) => {
                    assert_eq!(by_value_rule.rule, rule);
                }
                other => panic!("{rule}: the cause is {other:?}"),
            }
        }
    }

    /// Python's `EnqueueCommandV1` refuses an empty retention class key
    /// before any statement (`INVALID_INPUT`); a key that is not empty and
    /// that no class derives reaches the CHECK of the insert
    /// (`ENQUEUE_FAILED`).
    #[test]
    fn an_empty_retention_key_is_invalid_input_and_an_underived_one_a_failed_enqueue() {
        let task_id = Uuid::from_u128(7);
        let empty = send_retention("").expect_err("an empty key is refused");
        assert!(matches!(empty, RetentionKeyRefusal::Empty), "{empty:?}");
        match retention_refused(task_id, "send.task", empty) {
            SendOutcome::Invalid(record) => {
                assert_eq!(record.code, "INVALID_INPUT");
                assert_eq!(
                    record.message,
                    "enqueue input refused for send.task: retention_class_key must be non-empty"
                );
                match record.cause {
                    RefusalCauseRecord::ByValueRule(by_value_rule) => {
                        assert_eq!(by_value_rule.rule, "retention_class_key must be non-empty");
                    }
                    other => panic!("an empty key gave the cause {other:?}"),
                }
            }
            _ => panic!("an empty key is not SendValidationFailed"),
        }
        let underived = send_retention("standard_0d").expect_err("0 days is below the bounds");
        assert!(
            matches!(underived, RetentionKeyRefusal::NotDerived(_)),
            "{underived:?}"
        );
        match retention_refused(task_id, "send.task", underived) {
            SendOutcome::RetentionKeyRefused(record) => assert!(!record.retryable),
            _ => panic!("an underived key is not SendEnqueueFailed"),
        }
        assert!(matches!(
            send_retention("forever"),
            Ok(EnqueueRetention::Forever)
        ));
    }
}

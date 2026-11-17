//! The send operation: one enqueue of one task through the core.
//!
//! The Python client builds one frozen `EnqueueRequest` per send attempt: a
//! resend of the same task id passes the same values with the send kind
//! `REPEAT`, so a task that finished answers from history and does not run
//! again. The outcome is
//! `Enqueued` or one class per variant of the core's send error catalog
//! (`TaskSendError`), each with only the facts of its state.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use horsies_core::core::task::send_types::send_error_from_enqueue;
use horsies_core::{
    BrokerError, EnqueueRetention, RetentionChoice, SendKind, TaskSendError, TaskSendPayload,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use super::arguments::{
    utc_instant, RerunInputPolicy, RetentionArgument, RetentionRequest, StartArgument, StartRule,
};
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
    retention: RetentionRequest,
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
/// `idempotency_key` means no deadline, no options, no key. `send_kind` is
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
        retention,
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
        retention: RetentionArgument<'_>,
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
            retention: retention.request(),
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

/// The input or the options were refused. Nothing was written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SendValidationFailed {
    pub message: String,
}

/// The database failed during the enqueue; the row may or may not exist.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct SendEnqueueFailed {
    pub task_id: Uuid,
    pub message: String,
    pub retryable: bool,
}

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
    Refused(TaskSendError),
}

impl<'py> IntoPyObject<'py> for SendOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Enqueued(task_id) => Enqueued { task_id }.into_bound_py_any(py),
            Self::Refused(error) => refusal_record(py, error),
        }
    }
}

fn refusal_record(py: Python<'_>, error: TaskSendError) -> PyResult<Bound<'_, PyAny>> {
    match error {
        TaskSendError::SendSuppressed { message } => {
            SendSuppressed { message }.into_bound_py_any(py)
        }
        TaskSendError::ValidationFailed { message } => {
            SendValidationFailed { message }.into_bound_py_any(py)
        }
        TaskSendError::EnqueueFailed {
            task_id,
            message,
            retryable,
            ..
        } => SendEnqueueFailed {
            task_id,
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

/// The retention class of the enqueue, or the refusal of the period.
fn enqueue_retention(request: RetentionRequest) -> Result<EnqueueRetention, String> {
    match request {
        RetentionRequest::Default => Ok(EnqueueRetention::Default),
        RetentionRequest::Forever => Ok(EnqueueRetention::Forever),
        RetentionRequest::PeriodDays(days) => {
            match RetentionChoice::period(chrono::Duration::days(i64::from(days))) {
                Ok(RetentionChoice::Period(period)) => Ok(EnqueueRetention::Period(period)),
                Ok(RetentionChoice::Forever) => {
                    unreachable!("RetentionChoice::period returns a period or a refusal")
                }
                Err(refused) => Err(format!("retention period of {days} days: {refused}")),
            }
        }
    }
}

/// Enqueue one task. Writes; a caller that leaves early sees the outcome as
/// unknown, and a resend of the same request is safe.
pub async fn enqueue_operation(
    connection: BrokerConnection,
    command: Arc<EnqueueCommand>,
) -> SendOutcome {
    let retention = match enqueue_retention(command.retention) {
        Ok(retention) => retention,
        Err(message) => {
            return SendOutcome::Refused(TaskSendError::ValidationFailed { message });
        }
    };
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return refused(&command, retention, &error),
    };
    let (enqueued_at, enqueue_delay_seconds) = match command.start {
        StartRule::Now => (None, None),
        StartRule::AfterDelay { seconds } => (None, Some(seconds)),
        StartRule::At(at) => (Some(at), None),
    };
    let enqueued = broker
        .enqueue(
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
            command.rerun_input.keep(),
            command.send_kind,
        )
        .await;
    match enqueued {
        Ok(task_id) => SendOutcome::Enqueued(task_id),
        Err(error) => refused(&command, retention, &error),
    }
}

fn refused(
    command: &EnqueueCommand,
    retention: EnqueueRetention,
    error: &BrokerError,
) -> SendOutcome {
    SendOutcome::Refused(send_error_from_enqueue(
        command.task_id,
        command.payload(retention),
        error,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_and_forever_retentions_need_no_check() {
        assert_eq!(
            enqueue_retention(RetentionRequest::Default),
            Ok(EnqueueRetention::Default)
        );
        assert_eq!(
            enqueue_retention(RetentionRequest::Forever),
            Ok(EnqueueRetention::Forever)
        );
    }

    #[test]
    fn the_two_send_kinds_are_read_and_any_other_text_is_refused() {
        assert_eq!(send_kind_of("FIRST"), Ok(SendKind::First));
        assert_eq!(send_kind_of("REPEAT"), Ok(SendKind::Repeat));
        let refused = send_kind_of("repeat").expect_err("send kind texts are upper case");
        assert!(refused.contains("\"repeat\""), "{refused}");
    }

    #[test]
    fn a_period_outside_its_bound_is_refused_before_any_io() {
        assert!(enqueue_retention(RetentionRequest::PeriodDays(0)).is_err());
        assert!(enqueue_retention(RetentionRequest::PeriodDays(1_826)).is_err());
        assert!(matches!(
            enqueue_retention(RetentionRequest::PeriodDays(30)),
            Ok(EnqueueRetention::Period(_))
        ));
    }
}

//! The send operation: one enqueue of one task through the core.
//!
//! The Python client builds one frozen `EnqueueRequest` per send attempt: a
//! resend of the same task id passes the same values with the send kind
//! `REPEAT`, so a task that finished answers from history and does not run
//! again. The core's refusals that need no connection run first, then the
//! first send of a handle installs the schema once, as Python's broker does.
//! The outcome is `Enqueued`, or the broker catalog's class of the failure
//! (`horsies._catalog.broker_errors`: `BrokerInvalidInput`,
//! `BrokerInvalidTaskOptions`, `BrokerEnqueueFailed`,
//! `BrokerPayloadMismatch` or `BrokerIdempotencyKeyConflict`), built in Rust
//! (`catalog_errors`). The core decides the code, the message and the cause
//! of each failure (`BrokerOperationError::enqueue_error`), so the Python
//! client returns the class as it is.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use horsies_core::{
    BrokerError, BrokerOperationError, CheckedSend, EnqueueRefusal, EnqueueRetention, SendKind,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use uuid::Uuid;

use super::arguments::{utc_instant, RerunInputPolicy, StartArgument, StartRule};
use crate::handle::BrokerConnection;
use crate::records::{BrokerFailure, Outcome};

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

/// The subject of the exception of a failed enqueue.
const ENQUEUE_SUBJECT: &str = "enqueue";

/// The outcome of one send: `Enqueued`, or the broker catalog's class of
/// the failure, with `ENQUEUE_SUBJECT` as the subject of its exception.
pub type SendOutcome = Outcome<Enqueued>;

/// Enqueue one task. The core's refusals that need no connection run first
/// (`CheckedSend::check`), as `_prepare_enqueue` ran before the schema
/// install (`core/brokers/postgres.py` of the `v0.7.0` package): a refused send
/// answers `INVALID_INPUT` or `INVALID_TASK_OPTIONS` with no connect and no
/// statement. Then the schema is installed once per handle; a failed connect
/// or install is `ENQUEUE_FAILED`, as Python's enqueue reports it. Writes; a
/// caller that leaves early sees the outcome as unknown, and a resend of the
/// same request with the send kind `REPEAT` is safe.
pub async fn enqueue_operation(
    connection: BrokerConnection,
    command: Arc<EnqueueCommand>,
) -> SendOutcome {
    let retention = match send_retention(&command.retention_class_key) {
        Ok(retention) => retention,
        Err(error) => return refused(&command.task_name, error),
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
        Err(error) => return refused(&command.task_name, error),
    };
    let broker = match connection.initialized_broker().await {
        Ok(broker) => broker,
        Err(error) => return refused(&command.task_name, error),
    };
    match broker.enqueue_checked(&send, command.send_kind).await {
        Ok(task_id) => Outcome::Done(Enqueued { task_id }),
        Err(error) => refused(&command.task_name, error),
    }
}

/// The retention of the class key `class_key` of a send. Pure.
///
/// Errors: an empty key is the rule of the version-1 command
/// (`BrokerError::EnqueueContract`, `INVALID_INPUT`, as Python's
/// `EnqueueCommandV1` refuses it before any statement); a key that is not
/// empty and that no class derives is `BrokerError::RetentionClassKey`
/// (`ENQUEUE_FAILED`, not retryable, as Python's insert met the CHECK
/// `horsies_tasks_retention_class_key_form`). Nothing is written in both.
fn send_retention(class_key: &str) -> Result<EnqueueRetention, BrokerError> {
    match class_key.is_empty() {
        true => Err(BrokerError::EnqueueContract(
            EnqueueRefusal::empty_retention_class_key(),
        )),
        false => {
            EnqueueRetention::from_class_key(class_key).map_err(BrokerError::RetentionClassKey)
        }
    }
}

/// The outcome of a send of `task_name` that failed with `error`: the
/// broker catalog's class that the core decides
/// (`BrokerOperationError::enqueue_error`). Pure.
fn refused(task_name: &str, error: BrokerError) -> SendOutcome {
    Outcome::Failed(BrokerFailure::of_operation_error(
        ENQUEUE_SUBJECT,
        BrokerOperationError::enqueue_error(task_name, error),
    ))
}

#[cfg(test)]
mod tests {
    use horsies_core::{BrokerErrorCode, EnqueueRefusalCause, SendPart};

    use super::*;

    #[test]
    fn the_two_send_kinds_are_read_and_any_other_text_is_refused() {
        assert_eq!(send_kind_of("FIRST"), Ok(SendKind::First));
        assert_eq!(send_kind_of("REPEAT"), Ok(SendKind::Repeat));
        let refused = send_kind_of("repeat").expect_err("send kind texts are upper case");
        assert!(refused.contains("\"repeat\""), "{refused}");
    }

    /// The failure of a refused send, with the subject of its exception.
    fn failure_of(outcome: SendOutcome) -> BrokerFailure {
        match outcome {
            Outcome::Failed(failure) => failure,
            Outcome::Done(enqueued) => panic!("a refused send gave {enqueued:?}"),
        }
    }

    /// Python's `EnqueueCommandV1` refuses an empty retention class key
    /// before any statement (`INVALID_INPUT`, with the rule as the cause); a
    /// key that is not empty and that no class derives reaches the CHECK of
    /// the insert (`ENQUEUE_FAILED`, not retryable). Both are the failure of
    /// the send, with the subject `enqueue`.
    #[test]
    fn an_empty_retention_key_is_invalid_input_and_an_underived_one_a_failed_enqueue() {
        let empty = send_retention("").expect_err("an empty key is refused");
        match &empty {
            BrokerError::EnqueueContract(refusal) => assert_eq!(
                refusal.cause,
                EnqueueRefusalCause::CommandRuleRefused(
                    "retention_class_key must be non-empty".to_owned()
                )
            ),
            other => panic!("an empty key gave {other:?}"),
        }
        let failure = failure_of(refused("send.task", empty));
        assert_eq!(failure.subject, ENQUEUE_SUBJECT);
        assert_eq!(failure.error.code(), BrokerErrorCode::InvalidInput);
        assert_eq!(
            failure.error.message(),
            "enqueue input refused for send.task: retention_class_key must be non-empty"
        );

        let underived = send_retention("standard_0d").expect_err("0 days is below the bounds");
        assert!(
            matches!(underived, BrokerError::RetentionClassKey(_)),
            "{underived:?}"
        );
        let failure = failure_of(refused("send.task", underived));
        assert_eq!(failure.subject, ENQUEUE_SUBJECT);
        assert_eq!(failure.error.code(), BrokerErrorCode::EnqueueFailed);
        assert!(!failure.error.retryable());
        assert!(
            failure.error.message().starts_with(
                "Failed to enqueue task send.task: retention class key \"standard_0d\""
            ),
            "{}",
            failure.error.message()
        );

        assert!(matches!(
            send_retention("forever"),
            Ok(EnqueueRetention::Forever)
        ));
    }

    /// A core refusal of the task options is `INVALID_TASK_OPTIONS`, and of
    /// the input `INVALID_INPUT`: the core decides the code from the part.
    #[test]
    fn a_core_refusal_has_the_code_of_its_part() {
        for (part, code) in [
            (SendPart::Input, BrokerErrorCode::InvalidInput),
            (SendPart::TaskOptions, BrokerErrorCode::InvalidTaskOptions),
        ] {
            let refusal = EnqueueRefusal::by_rule(part, "refused".to_owned());
            let failure = failure_of(refused("t", BrokerError::EnqueueContract(refusal)));
            assert_eq!(failure.error.code(), code, "{part:?}");
            assert_eq!(
                failure.error.message(),
                "enqueue contract violation: refused"
            );
        }
    }
}

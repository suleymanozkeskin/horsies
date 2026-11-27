//! The send: the rules that each send of an accepted config applies, and the
//! enqueue of one prepared send.
//!
//! - `SendRules.prepareSendFacts` is pure apart from the task id that it
//!   mints and the log: the retention of the send (the Rust client's rule
//!   of `send::options::send_retention`), the payload bound
//!   (`enforce_payload_policy`, which logs the core's warning once per task
//!   name), the task id, the stored options text with the deadline, and the
//!   priority of the task's queue in the accepted config. The task name and
//!   the queue come from the checked options of the task. A refused fact is
//!   the send error value, not a throw. It starts the log bridge of the
//!   process first (the forwarder thread, not the core), so the first
//!   warning of a task is a record of the log sink.
//! - `enqueue` is one send through the core, as the Rust client's
//!   `enqueue_once`: the core's checks that need no connection run first
//!   (`CheckedSend::check`, which keeps the shared-class timeout check for a
//!   `REPEAT` send too), then the schema install once per handle, then the
//!   write. In a task child the send goes through the child host's producer
//!   handle, else through the given handle (`ProducerHandle`). Every
//!   failure maps to the task send catalog by the core's one rule
//!   (`send_error_from_enqueue`). Writes: a stop of the core in flight
//!   leaves the outcome unknown, and a resend of the same request with
//!   `REPEAT` is safe.

use chrono::{DateTime, Utc};
use horsies_core::core::config::payload::{enforce_payload_policy, PayloadKind};
use horsies_core::core::task::send_types::{
    send_error_from_enqueue, send_failure_exception, SendFailureException,
};
use horsies_core::handle::CoreHandle as LazyHandle;
use horsies_core::telemetry::trace_carrier::SendTrace;
use horsies_core::{
    BrokerError, CheckedSend, CustomQueueConfig, EnqueueRetention, FailureCause, PayloadPolicy,
    QueueMode, RetentionConfig, RetentionDays, SendKind as CoreSendKind, TaskOptions,
    TaskSendError, TaskSendPayload,
};
use napi::bindgen_prelude::{BigInt, Object};
use napi::Env;
use napi_derive::napi;
use uuid::Uuid;

use crate::call::guard::{sync_call, CallKind};
use crate::call::operation::{guarded_start, spawn_operation};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::handle::core_handle::CoreHandle;
use crate::logging::bridge::{started_log, LogBridge};
use crate::process_core::runtime::{pure_call_refusal, CallRefusal, StartFailure};
use crate::sends::early_end::thrown_now;
use crate::sends::producer_handle::ProducerHandle;
use crate::sends::task_options::TaskOptionsRecord;
use crate::values::boundary::{
    instant_of_epoch_micros, text_of_uuid, u32_of_number, uuid_of_text, BoundaryRefusal,
};
use crate::values::catalog_values::RawCatalogError;
use crate::values::trace_carrier::TraceArgument;

/// The subject of the exception of a refused prepared send.
const PREPARE_SUBJECT: &str = "prepare_send";

/// The subject of the exception of a failed enqueue.
const ENQUEUE_SUBJECT: &str = "enqueue";

/// The largest byte count that crosses as a JavaScript number with no lost
/// digit: 2^53 - 1.
const MAX_EXACT_BYTE_COUNT: f64 = 9_007_199_254_740_991.0;

/// The rules of an accepted app config that each send applies: the retention
/// config, the payload policy and the priority of each queue. JavaScript
/// gets it from `checkAppConfig`; it has no constructor.
#[napi]
#[derive(Clone, Debug)]
pub struct SendRules {
    retention: RetentionConfig,
    payload: PayloadPolicy,
    queues: QueuePriorities,
}

impl SendRules {
    /// The rules of an accepted config. Pure.
    pub fn new(
        retention: RetentionConfig,
        payload: PayloadPolicy,
        queues: QueuePriorities,
    ) -> Self {
        Self {
            retention,
            payload,
            queues,
        }
    }

    /// The retention config of the accepted app config. Pure.
    pub fn retention(&self) -> &RetentionConfig {
        &self.retention
    }

    /// The priority of a send on `queue`, or the refusal of a queue that the
    /// custom mode does not hold. Pure.
    pub fn queue_priority(&self, queue: &str) -> Result<SendPriority, BoundaryRefusal> {
        self.queues.priority_of(queue)
    }
}

/// The queues of an accepted config, as the priority of a send reads them.
#[derive(Clone, Debug)]
pub enum QueuePriorities {
    /// The default queue mode: one queue, whose priority the package holds.
    DefaultMode,
    /// The custom queue mode: each queue with the priority that the core's
    /// config holds (the core's default for a queue that names none).
    Custom(Vec<CustomQueueConfig>),
}

impl QueuePriorities {
    /// The queues of a config that `AppConfig::validate` accepted. That
    /// check refuses the custom mode with no queue list, so such a config
    /// has no queue here, and each send of it is refused. Pure.
    pub fn of(mode: QueueMode, custom_queues: Option<Vec<CustomQueueConfig>>) -> Self {
        match (mode, custom_queues) {
            (QueueMode::Default, _) => Self::DefaultMode,
            (QueueMode::Custom, Some(queues)) => Self::Custom(queues),
            (QueueMode::Custom, None) => Self::Custom(Vec::new()),
        }
    }

    /// The priority of a send on `queue`, or the refusal of a queue that the
    /// custom mode does not hold: the package sends only on the queue of a
    /// listed task, and `Horsies.create` checks that queue. Pure.
    fn priority_of(&self, queue: &str) -> Result<SendPriority, BoundaryRefusal> {
        match self {
            Self::DefaultMode => Ok(SendPriority::DefaultQueue),
            Self::Custom(queues) => queues
                .iter()
                .find(|configured| configured.name == queue)
                .map(|configured| SendPriority::Configured {
                    priority: configured.priority,
                })
                .ok_or_else(|| {
                    BoundaryRefusal::new(
                        "options.queueName",
                        "a custom queue of the accepted config",
                    )
                }),
        }
    }
}

/// The priority that a send carries.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendPriority {
    /// The one queue of the default queue mode: the package holds its
    /// priority.
    DefaultQueue,
    /// A custom queue: its priority in the accepted config.
    Configured { priority: u32 },
}

/// The retention that a send chooses.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum RetentionChoice {
    /// The queue mapping of the config, else the 30-day default.
    QueueOrDefault,
    Forever,
    /// A period of whole days that the config registers.
    Days {
        days: f64,
    },
}

/// The retention of a send as the core stores it.
#[napi(discriminant = "kind", discriminant_case = "camelCase")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetentionRecord {
    Forever,
    /// A class key: `standard_<days>d`.
    ClassKey {
        key: String,
    },
}

impl RetentionRecord {
    /// The record of the core's retention. Pure.
    fn of(retention: EnqueueRetention) -> Self {
        match retention {
            EnqueueRetention::Forever => Self::Forever,
            EnqueueRetention::Default | EnqueueRetention::Period(_) => Self::ClassKey {
                key: retention.class_key(),
            },
        }
    }

    /// The core's retention of the record, or the refusal of a class key that
    /// no class derives: the package passes only keys that the core gave.
    /// Pure.
    fn retention(&self) -> Result<EnqueueRetention, BoundaryRefusal> {
        match self {
            Self::Forever => Ok(EnqueueRetention::Forever),
            Self::ClassKey { key } => EnqueueRetention::from_class_key(key)
                .map_err(|refused| BoundaryRefusal::new("retention.key", &refused.to_string())),
        }
    }
}

/// A deadline: none, or an instant in epoch microseconds.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum DeadlineFact {
    NoDeadline,
    Until { epoch_micros: BigInt },
}

impl DeadlineFact {
    /// The deadline, or the refusal of microseconds outside the instant
    /// range. Pure.
    fn instant(&self) -> Result<Option<DateTime<Utc>>, BoundaryRefusal> {
        match self {
            Self::NoDeadline => Ok(None),
            Self::Until { epoch_micros } => {
                instant_of_epoch_micros("deadline.epochMicros", epoch_micros).map(Some)
            }
        }
    }
}

/// The facts of one send that the send rules judge. The task name and the
/// queue are the checked options' own.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct SendFacts {
    pub retention: RetentionChoice,
    pub deadline: DeadlineFact,
    /// The UTF-8 byte length of the input text.
    pub input_bytes: f64,
}

/// The prepared facts of one send, or the send error of a refused fact.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreparedSend {
    Prepared {
        task_id: String,
        task_options: String,
        retention: RetentionRecord,
        priority: SendPriority,
    },
    Refused {
        error: RawCatalogError,
    },
}

#[napi]
impl SendRules {
    /// The prepared facts of one send of the task of the checked `options`,
    /// on its queue. Mints a task id; the fork and stop rules apply, as for
    /// every core call. Starts the log bridge of the process (the forwarder thread), so
    /// that the payload warning reaches the log sink; starts no core. A
    /// forwarder thread that cannot start throws `CORE_START_FAILED`, as the
    /// first core call would.
    #[napi(ts_return_type = "NativeEnvelope<PreparedSend>")]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "napi-rs passes the arguments of a JavaScript call by value"
    )]
    pub fn prepare_send_facts(
        &self,
        facts: SendFacts,
        options: &TaskOptionsRecord,
    ) -> Envelope<PreparedSend> {
        sync_call("prepare_send_facts", || {
            if let Some(refusal) = pure_call_refusal() {
                return Envelope::Thrown(refusal.thrown());
            }
            let log = match started_log() {
                Ok(log) => log,
                Err(failure) => {
                    return Envelope::Thrown(
                        CallRefusal::StartFailed(StartFailure::LogForwarderStartFailed {
                            cause: failure.cause,
                        })
                        .thrown(),
                    )
                }
            };
            match self.prepared(log, &facts, options.options()) {
                Ok(prepared) => Envelope::Value(prepared),
                Err(refusal) => Envelope::Thrown(refusal.into()),
            }
        })
    }
}

impl SendRules {
    /// The prepared send, in the Rust client's order: the retention, the
    /// payload bound, the task id, the options text; and the priority of the
    /// queue. A value that breaks a boundary rule is the refusal: checked
    /// options with no queue, or a queue that the config does not hold.
    /// Mints a task id; the payload warning goes to `log`.
    fn prepared(
        &self,
        log: &LogBridge,
        facts: &SendFacts,
        options: &TaskOptions,
    ) -> Result<PreparedSend, BoundaryRefusal> {
        let deadline = facts.deadline.instant()?;
        let input_bytes = byte_count("inputBytes", facts.input_bytes)?;
        let task_name = options.task_name.as_str();
        let queue = options.queue_name.as_deref().ok_or_else(|| {
            BoundaryRefusal::new("options", "checked options that name the queue of the task")
        })?;
        let priority = self.queues.priority_of(queue)?;
        let retention = match self.retention_of(queue, &facts.retention) {
            Ok(retention) => retention,
            Err(cause) => {
                return Ok(refused(&validation_failed(
                    task_name,
                    &format!("Invalid retention: {cause}"),
                )))
            }
        };
        if let Some(too_large) = self.payload_refusal(log, task_name, input_bytes) {
            return Ok(refused(&too_large));
        }
        let task_id = match horsies_core::mint_task_id() {
            Ok(task_id) => task_id,
            Err(error) => {
                return Ok(refused(&validation_failed(
                    task_name,
                    &format!("task id mint failed: {error}"),
                )))
            }
        };
        Ok(PreparedSend::Prepared {
            task_id: text_of_uuid(task_id),
            task_options: match deadline {
                None => options.stored_options_text(),
                Some(at) => options.stored_send_options_text(at),
            },
            retention: RetentionRecord::of(retention),
            priority,
        })
    }

    /// The retention of a send on `queue`: the send's own choice first, then
    /// the queue mapping, then the 30-day default. A chosen period must be
    /// one of the sendable periods. The error is the cause text. Pure.
    fn retention_of(
        &self,
        queue: &str,
        choice: &RetentionChoice,
    ) -> Result<EnqueueRetention, String> {
        match choice {
            RetentionChoice::QueueOrDefault => Ok(self.retention.resolve_queue_retention(queue)),
            RetentionChoice::Forever => Ok(EnqueueRetention::Forever),
            RetentionChoice::Days { days } => {
                let period = period_of(*days)?;
                let allowed = self.retention.sendable_periods();
                match allowed.contains(&period) {
                    true => Ok(EnqueueRetention::Period(period)),
                    false => Err(format!(
                        "retention of {} days is not a period this app registers; leave \
                         retention out for the queue mapping or the 30-day default, choose \
                         forever, or one of: {:?} days. Add the period to the retention \
                         periods of the app config to send with it",
                        period.get(),
                        allowed.iter().map(|days| days.get()).collect::<Vec<u16>>()
                    )),
                }
            }
        }
    }

    /// The send error of an input above the reject bound, or `None`. Logs
    /// the core's warning above the warn bound to `log`, whatever the
    /// default dispatcher of the thread is. Pure apart from the log.
    fn payload_refusal(
        &self,
        log: &LogBridge,
        task_name: &str,
        input_bytes: usize,
    ) -> Option<TaskSendError> {
        let size = tracing::dispatcher::with_default(log.dispatch(), || {
            enforce_payload_policy(&self.payload, task_name, PayloadKind::Kwargs, input_bytes)
        })?;
        let limit_bytes = self.payload.reject_bytes.unwrap_or_else(|| {
            unreachable!("the payload policy refuses only above its reject bound")
        });
        let size_bytes = u64::try_from(size).unwrap_or(u64::MAX);
        Some(TaskSendError::PayloadTooLarge {
            message: format!(
                "input payload for {task_name} is {size_bytes} bytes, above the payload reject \
                 bound of {limit_bytes} bytes; nothing was enqueued. Pass a reference to \
                 external storage or raise the bound."
            ),
            size_bytes,
            limit_bytes,
        })
    }
}

/// The retention period of a number of days, or the cause of its refusal.
/// Pure.
fn period_of(days: f64) -> Result<RetentionDays, String> {
    let whole = u32_of_number("retention.days", days)
        .map_err(|refusal| format!("{}: {}", refusal.subject, refusal.rule))?;
    RetentionDays::parse_period(chrono::Duration::days(i64::from(whole)))
        .map_err(|refused| refused.to_string())
}

/// The byte count that a number holds, or the refusal of `subject`: a
/// whole number from 0 to 2^53 - 1. Pure.
fn byte_count(subject: &str, value: f64) -> Result<usize, BoundaryRefusal> {
    let refused = || BoundaryRefusal::new(subject, "a whole number from 0 to 9007199254740991");
    match value.is_finite() && value.fract() == 0.0 && (0.0..=MAX_EXACT_BYTE_COUNT).contains(&value)
    {
        // The value is whole and in range, so the conversion is exact.
        true => usize::try_from(value as u64).map_err(|_| refused()),
        false => Err(refused()),
    }
}

/// The send error of a fact that a check refused before anything was
/// written (the Rust client's `validation_failed`). Pure.
fn validation_failed(task_name: &str, message: &str) -> TaskSendError {
    TaskSendError::ValidationFailed {
        message: format!("{message} (task {task_name})"),
        cause: FailureCause::PreconditionFailed,
    }
}

/// The prepared send of a send error. Pure.
fn refused(error: &TaskSendError) -> PreparedSend {
    PreparedSend::Refused {
        error: RawCatalogError::of(PREPARE_SUBJECT, error),
    }
}

/// A delay before a worker may start the task.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum DelayFact {
    NoDelay,
    Seconds { seconds: f64 },
}

/// A text that a send or a task error may leave out.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OptionalText {
    None,
    Text { text: String },
}

impl OptionalText {
    /// The text, or `None`. Pure.
    pub fn text(self) -> Option<String> {
        match self {
            Self::None => None,
            Self::Text { text } => Some(text),
        }
    }
}

/// One send, as the core takes it. A resend of the same task id passes the
/// same values.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct EnqueueRequest {
    pub task_id: String,
    pub task_name: String,
    pub queue_name: String,
    pub priority: f64,
    pub input_json: String,
    pub sent_at_micros: BigInt,
    pub deadline: DeadlineFact,
    pub delay: DelayFact,
    pub task_options: OptionalText,
    pub idempotency_key: OptionalText,
    pub retention: RetentionRecord,
}

/// The first send of a task id, or a resend of it.
#[napi(string_enum = "UPPERCASE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendKind {
    First,
    Repeat,
}

impl SendKind {
    /// The core's send kind. Pure.
    fn core(self) -> CoreSendKind {
        match self {
            Self::First => CoreSendKind::First,
            Self::Repeat => CoreSendKind::Repeat,
        }
    }
}

/// The outcome of one send: the task id, or the send error (a class of the
/// task send catalog).
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Enqueued { task_id: String },
    Failed { error: RawCatalogError },
}

/// One checked send: the task id, the payload of a resend, the send kind and
/// the trace.
#[derive(Clone, Debug)]
struct EnqueueCommand {
    task_id: Uuid,
    payload: TaskSendPayload,
    send_kind: CoreSendKind,
    trace: SendTrace,
}

/// Send one task on `handle`: in a task child through the child host's
/// producer handle, else through `handle`. A request value that breaks a
/// boundary rule resolves with `CLIENT_DEFECT` at once: the package checks
/// each value first.
#[napi(ts_return_type = "Promise<NativeEnvelope<EnqueueOutcome>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn enqueue<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    request: EnqueueRequest,
    send_kind: SendKind,
    trace: TraceArgument,
) -> napi::Result<Object<'env>> {
    enqueue_on(
        env,
        ProducerHandle::of_process(handle),
        request,
        send_kind,
        trace,
    )
}

/// Send one task through `producer`, as `enqueue` does.
///
/// Failure exits: the `napi::Error` of a Promise that Node-API cannot
/// create. Every other end is an envelope.
pub fn enqueue_on<'env>(
    env: &'env Env,
    producer: ProducerHandle,
    request: EnqueueRequest,
    send_kind: SendKind,
    trace: TraceArgument,
) -> napi::Result<Object<'env>> {
    let command = guarded_start(ENQUEUE_SUBJECT, || {
        command_of(request, send_kind, trace).map_err(NativeThrown::from)
    });
    match command {
        Err(thrown) => thrown_now::<EnqueueOutcome>(env, thrown),
        Ok(command) => spawn_operation(env, ENQUEUE_SUBJECT, CallKind::Write, move |_core| {
            enqueue_operation(producer, command)
        }),
    }
}

/// The checked command of a request, or the refusal of its first value that
/// breaks a boundary rule. Pure.
fn command_of(
    request: EnqueueRequest,
    send_kind: SendKind,
    trace: TraceArgument,
) -> Result<EnqueueCommand, BoundaryRefusal> {
    let priority = u32_of_number("priority", request.priority).and_then(|value| {
        i32::try_from(value)
            .map_err(|_| BoundaryRefusal::new("priority", "a whole number from 0 to 2147483647"))
    })?;
    let enqueue_delay_seconds = match request.delay {
        DelayFact::NoDelay => None,
        DelayFact::Seconds { seconds } => Some(i64::from(u32_of_number("delay.seconds", seconds)?)),
    };
    Ok(EnqueueCommand {
        task_id: uuid_of_text("taskId", &request.task_id)?,
        payload: TaskSendPayload {
            task_name: request.task_name,
            queue_name: request.queue_name,
            priority,
            input_json: request.input_json,
            sent_at: instant_of_epoch_micros("sentAtMicros", &request.sent_at_micros)?,
            good_until: request.deadline.instant()?,
            enqueue_delay_seconds,
            task_options: request.task_options.text(),
            idempotency_key: request.idempotency_key.text(),
            retention: request.retention.retention()?,
        },
        send_kind: send_kind.core(),
        trace: trace.send_trace()?,
    })
}

/// One send through `producer`, as the Rust client's `enqueue_once`: the
/// core's check of the send (no connection), the schema install once per
/// handle, then the write. The outcome is named by the send catalog.
async fn enqueue_operation(producer: ProducerHandle, command: EnqueueCommand) -> EnqueueOutcome {
    let handle = producer.lazy_handle();
    let send = match checked_send(handle, &command) {
        Ok(send) => send,
        Err(error) => return enqueue_failed(&command, &error),
    };
    match handle.initialized_broker().await {
        Ok(broker) => enqueue_outcome_of(
            &command,
            broker.enqueue_checked(&send, command.send_kind).await,
        ),
        Err(error) => enqueue_failed(&command, &error),
    }
}

/// The outcome of the write of `command`: the stored task id, or the send
/// error of the failure. Pure.
fn enqueue_outcome_of(
    command: &EnqueueCommand,
    written: Result<Uuid, BrokerError>,
) -> EnqueueOutcome {
    match written {
        Ok(task_id) => EnqueueOutcome::Enqueued {
            task_id: text_of_uuid(task_id),
        },
        Err(error) => enqueue_failed(command, &error),
    }
}

/// The outcome of a send of `command` that failed before or at the write:
/// the core's check refused it, the connect or the schema install failed,
/// or the write failed. Pure.
fn enqueue_failed(command: &EnqueueCommand, error: &BrokerError) -> EnqueueOutcome {
    EnqueueOutcome::Failed {
        error: enqueue_failure_record(command, error),
    }
}

/// The raw record of the send error of a failed enqueue of `command`: the
/// class by the core's one rule (`send_error_from_enqueue`), and in the
/// `cause` slot the exception that Python stores for `error`
/// (`send_failure_exception`). The send error holds no rule text, no
/// reader text and no data check, so its own slot is not exact for each
/// error. Pure.
fn enqueue_failure_record(command: &EnqueueCommand, error: &BrokerError) -> RawCatalogError {
    let send_error = send_error_from_enqueue(command.task_id, command.payload.clone(), error);
    match send_failure_exception(error) {
        SendFailureException::Slot(exception) => {
            RawCatalogError::of_with_exception(ENQUEUE_SUBJECT, &send_error, &exception)
        }
        SendFailureException::NoSlot => RawCatalogError::of(ENQUEUE_SUBJECT, &send_error),
    }
}

/// The core's checks of the send of `command` that need no connection, with
/// the rerun-input default of `handle`'s manifest. Touches no database;
/// reads the clock.
fn checked_send<'command>(
    handle: &LazyHandle,
    command: &'command EnqueueCommand,
) -> Result<CheckedSend<'command>, BrokerError> {
    let payload = &command.payload;
    Ok(CheckedSend::check(
        &payload.task_name,
        &payload.input_json,
        &payload.queue_name,
        payload.priority,
        Some(payload.sent_at),
        None,
        payload.good_until,
        payload.task_options.as_deref(),
        Some(command.task_id),
        payload.enqueue_delay_seconds,
        payload.idempotency_key.as_deref(),
        payload.retention,
        handle.manifest().send.retain_rerun_input_default,
    )?
    .with_trace(command.trace.clone()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use horsies_core::catalog::CodeEnum;

    use super::*;
    use crate::logging::levels::RecordLevel;
    use crate::logging::names::LoggerName;
    use crate::values::boundary::bigint_of_i64;
    use crate::values::catalog_values::{ClientClassName, ExceptionRecord, RawSlotValue};
    use crate::values::core_failures::CauseRecord;

    /// A log bridge of a test that reads none of its records.
    fn quiet_log() -> LogBridge {
        let (log, _records) = LogBridge::new();
        log
    }

    fn rules() -> SendRules {
        let mut retention = RetentionConfig {
            retention_periods: vec![chrono::Duration::days(7)],
            ..RetentionConfig::default()
        };
        let _previous = retention.queue_retention.insert("audit".to_owned(), None);
        SendRules::new(
            retention,
            PayloadPolicy {
                warn_bytes: None,
                reject_bytes: Some(100),
            },
            QueuePriorities::DefaultMode,
        )
    }

    fn options() -> TaskOptions {
        TaskOptions {
            task_name: "report".to_owned(),
            queue_name: Some("default".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: horsies_core::Execution::Isolated,
            cancel_grace_ms: None,
        }
    }

    fn facts(retention: RetentionChoice, input_bytes: f64) -> SendFacts {
        SendFacts {
            retention,
            deadline: DeadlineFact::NoDeadline,
            input_bytes,
        }
    }

    fn refused_class(prepared: PreparedSend) -> String {
        match prepared {
            PreparedSend::Refused { error } => error.class,
            PreparedSend::Prepared { .. } => panic!("the send is refused"),
        }
    }

    fn prepared_retention(prepared: PreparedSend) -> RetentionRecord {
        match prepared {
            PreparedSend::Prepared { retention, .. } => retention,
            PreparedSend::Refused { error } => panic!("the send is prepared, got {error:?}"),
        }
    }

    #[test]
    fn a_send_with_no_choice_takes_the_queue_mapping_or_the_default_class() {
        let prepared = rules()
            .prepared(
                &quiet_log(),
                &facts(RetentionChoice::QueueOrDefault, 10.0),
                &options(),
            )
            .expect("the facts cross");
        assert_eq!(
            prepared_retention(prepared),
            RetentionRecord::ClassKey {
                key: "standard_30d".to_owned()
            }
        );
        let audit = TaskOptions {
            queue_name: Some("audit".to_owned()),
            ..options()
        };
        let prepared = rules()
            .prepared(
                &quiet_log(),
                &facts(RetentionChoice::QueueOrDefault, 10.0),
                &audit,
            )
            .expect("the facts cross");
        assert_eq!(prepared_retention(prepared), RetentionRecord::Forever);
    }

    #[test]
    fn a_registered_period_is_its_class_and_another_period_is_a_validation_failure() {
        let prepared = rules()
            .prepared(
                &quiet_log(),
                &facts(RetentionChoice::Days { days: 7.0 }, 10.0),
                &options(),
            )
            .expect("the facts cross");
        assert_eq!(
            prepared_retention(prepared),
            RetentionRecord::ClassKey {
                key: "standard_7d".to_owned()
            }
        );
        let refused = rules()
            .prepared(
                &quiet_log(),
                &facts(RetentionChoice::Days { days: 9.0 }, 10.0),
                &options(),
            )
            .expect("the facts cross");
        assert_eq!(refused_class(refused), "TaskSendValidationFailed");
    }

    #[test]
    fn an_input_above_the_reject_bound_is_payload_too_large_with_its_sizes() {
        let refused = rules()
            .prepared(
                &quiet_log(),
                &facts(RetentionChoice::Forever, 101.0),
                &options(),
            )
            .expect("the facts cross");
        let PreparedSend::Refused { error } = refused else {
            panic!("an input above the bound is refused");
        };
        assert_eq!(error.class, "TaskSendPayloadTooLarge");
        let integers: BTreeSet<i128> = error
            .slots
            .iter()
            .filter_map(|slot| match &slot.value {
                RawSlotValue::Integer { value } => Some(value.get_i128().0),
                _other => None,
            })
            .collect();
        assert_eq!(integers, BTreeSet::from([100, 101]));
    }

    #[test]
    fn a_prepared_send_with_a_deadline_stores_it_in_the_options_text() {
        let deadline = SendFacts {
            deadline: DeadlineFact::Until {
                epoch_micros: bigint_of_i64(1_786_525_812_654_321),
            },
            ..facts(RetentionChoice::Forever, 10.0)
        };
        let PreparedSend::Prepared { task_options, .. } = rules()
            .prepared(&quiet_log(), &deadline, &options())
            .expect("the facts cross")
        else {
            panic!("the send is prepared");
        };
        assert!(
            task_options.contains(r#""good_until":"2026-08-12T09:10:12.654321+00:00""#),
            "{task_options}"
        );
    }

    /// The first input of a task above the warn bound is a WARNING record
    /// of the log bridge that the send rules get, not an event that no
    /// dispatcher takes. The task name is used by no other test: the core
    /// warns once per task name in a process.
    #[test]
    fn the_first_input_above_the_warn_bound_is_a_warning_record() {
        let (log, records) = LogBridge::new();
        let rules = SendRules::new(
            RetentionConfig::default(),
            PayloadPolicy {
                warn_bytes: Some(10),
                reject_bytes: None,
            },
            QueuePriorities::DefaultMode,
        );
        let first_warning = TaskOptions {
            task_name: "first_payload_warning".to_owned(),
            ..options()
        };
        let prepared = rules
            .prepared(&log, &facts(RetentionChoice::Forever, 11.0), &first_warning)
            .expect("the facts cross");
        assert!(matches!(prepared, PreparedSend::Prepared { .. }));
        let record = records
            .try_recv()
            .expect("the first warning reached the channel");
        assert_eq!(record.logger, LoggerName::App);
        assert_eq!(record.level, RecordLevel::Warning);
        assert!(
            record.message.starts_with("payload size guardrail"),
            "{}",
            record.message
        );
    }

    fn prepared_priority(rules: &SendRules, options: &TaskOptions) -> SendPriority {
        match rules
            .prepared(
                &quiet_log(),
                &facts(RetentionChoice::Forever, 10.0),
                options,
            )
            .expect("the facts cross")
        {
            PreparedSend::Prepared { priority, .. } => priority,
            PreparedSend::Refused { error } => panic!("the send is prepared, got {error:?}"),
        }
    }

    /// The rules of a custom config with the queues of the core's config
    /// texts: `billing` with its own priority, `bulk` with none.
    fn custom_rules() -> SendRules {
        let queues: Vec<CustomQueueConfig> =
            serde_json::from_str(r#"[{"name": "billing", "priority": 7}, {"name": "bulk"}]"#)
                .expect("the queue texts read as the core's queue config");
        SendRules::new(
            RetentionConfig::default(),
            PayloadPolicy::default(),
            QueuePriorities::of(QueueMode::Custom, Some(queues)),
        )
    }

    #[test]
    fn a_send_in_the_default_queue_mode_takes_the_priority_of_the_package() {
        assert_eq!(
            prepared_priority(&rules(), &options()),
            SendPriority::DefaultQueue
        );
        assert!(matches!(
            QueuePriorities::of(QueueMode::Default, None),
            QueuePriorities::DefaultMode
        ));
    }

    #[test]
    fn a_send_on_a_custom_queue_takes_the_priority_of_the_core_config() {
        let rules = custom_rules();
        let billing = TaskOptions {
            queue_name: Some("billing".to_owned()),
            ..options()
        };
        let bulk = TaskOptions {
            queue_name: Some("bulk".to_owned()),
            ..options()
        };
        assert_eq!(
            prepared_priority(&rules, &billing),
            SendPriority::Configured { priority: 7 }
        );
        // The core's default for a queue that names no priority.
        assert_eq!(
            prepared_priority(&rules, &bulk),
            SendPriority::Configured { priority: 1 }
        );
    }

    #[test]
    fn a_queue_that_the_custom_config_does_not_hold_is_a_boundary_refusal() {
        let unknown = TaskOptions {
            queue_name: Some("unknown".to_owned()),
            ..options()
        };
        let refused = custom_rules().prepared(
            &quiet_log(),
            &facts(RetentionChoice::Forever, 10.0),
            &unknown,
        );
        assert!(matches!(refused, Err(refusal) if refusal.subject == "options.queueName"));
        let no_queue = TaskOptions {
            queue_name: None,
            ..options()
        };
        let refused = rules().prepared(
            &quiet_log(),
            &facts(RetentionChoice::Forever, 10.0),
            &no_queue,
        );
        assert!(matches!(refused, Err(refusal) if refusal.subject == "options"));
    }

    #[test]
    fn a_byte_count_that_is_not_a_whole_number_is_a_boundary_refusal() {
        for value in [-1.0, 1.5, f64::NAN, 9_007_199_254_740_992.0] {
            assert!(byte_count("inputBytes", value).is_err(), "{value}");
        }
        assert_eq!(byte_count("inputBytes", 0.0), Ok(0));
    }

    #[test]
    fn a_request_keeps_its_values_in_the_payload_of_a_resend() {
        let command = command_of(
            EnqueueRequest {
                task_id: "01890a5d-ac96-774b-bcce-b302099a8057".to_owned(),
                task_name: "report".to_owned(),
                queue_name: "default".to_owned(),
                priority: 100.0,
                input_json: "{}".to_owned(),
                sent_at_micros: bigint_of_i64(1_786_525_812_654_321),
                deadline: DeadlineFact::NoDeadline,
                delay: DelayFact::Seconds { seconds: 30.0 },
                task_options: OptionalText::None,
                idempotency_key: OptionalText::Text {
                    text: "report:1".to_owned(),
                },
                retention: RetentionRecord::ClassKey {
                    key: "standard_30d".to_owned(),
                },
            },
            SendKind::Repeat,
            TraceArgument::NoCarrier,
        )
        .expect("the request crosses");
        assert_eq!(command.send_kind, CoreSendKind::Repeat);
        assert_eq!(command.payload.enqueue_delay_seconds, Some(30));
        assert_eq!(command.payload.idempotency_key.as_deref(), Some("report:1"));
        assert_eq!(command.payload.task_options, None);
        assert_eq!(command.payload.retention.class_key(), "standard_30d");
    }

    #[test]
    fn a_class_key_that_no_class_derives_is_a_boundary_refusal() {
        let record = RetentionRecord::ClassKey {
            key: "standard_0d".to_owned(),
        };
        assert!(matches!(record.retention(), Err(refusal) if refusal.subject == "retention.key"));
    }

    #[test]
    fn a_failed_enqueue_names_its_send_catalog_class() {
        let payload = TaskSendPayload {
            task_name: "report".to_owned(),
            queue_name: "default".to_owned(),
            priority: 100,
            input_json: "{}".to_owned(),
            sent_at: Utc::now(),
            good_until: None,
            enqueue_delay_seconds: None,
            task_options: None,
            idempotency_key: None,
            retention: EnqueueRetention::Default,
        };
        let error =
            send_error_from_enqueue(Uuid::from_u128(7), payload, &BrokerError::ListenerClosed);
        let raw = RawCatalogError::of(ENQUEUE_SUBJECT, &error);
        assert_eq!(raw.class, "TaskSendEnqueueFailed");
        assert_eq!(error.code().wire_value(), "ENQUEUE_FAILED");
    }

    /// A first send of the task `report` with the input `{}`.
    fn report_command() -> EnqueueCommand {
        command_of(
            EnqueueRequest {
                task_id: "01890a5d-ac96-774b-bcce-b302099a8057".to_owned(),
                task_name: "report".to_owned(),
                queue_name: "default".to_owned(),
                priority: 100.0,
                input_json: "{}".to_owned(),
                sent_at_micros: bigint_of_i64(1_786_525_812_654_321),
                deadline: DeadlineFact::NoDeadline,
                delay: DelayFact::NoDelay,
                task_options: OptionalText::None,
                idempotency_key: OptionalText::None,
                retention: RetentionRecord::ClassKey {
                    key: "standard_30d".to_owned(),
                },
            },
            SendKind::First,
            TraceArgument::NoCarrier,
        )
        .expect("the request crosses")
    }

    /// A connect or a schema install that fails (here: the listener closed)
    /// is the send error `ENQUEUE_FAILED`, retryable, with the core's class
    /// in the `cause` slot.
    #[test]
    fn a_failed_connect_or_install_is_enqueue_failed() {
        let command = report_command();
        let EnqueueOutcome::Failed { error } =
            enqueue_failed(&command, &BrokerError::ListenerClosed)
        else {
            panic!("a failed install is a send error");
        };
        assert_eq!(error.class, "TaskSendEnqueueFailed");
        assert!(error.slots.iter().any(
            |slot| slot.name == "retryable" && slot.value == RawSlotValue::Flag { value: true }
        ));
    }

    #[test]
    fn a_written_send_is_enqueued_with_its_task_id_and_a_failed_write_is_its_send_error() {
        let command = report_command();
        assert_eq!(
            enqueue_outcome_of(&command, Ok(command.task_id)),
            EnqueueOutcome::Enqueued {
                task_id: "01890a5d-ac96-774b-bcce-b302099a8057".to_owned()
            }
        );
        assert_eq!(
            enqueue_outcome_of(&command, Err(BrokerError::ListenerClosed)),
            enqueue_failed(&command, &BrokerError::ListenerClosed)
        );
    }

    /// A refusal of the core before any statement, of the input part.
    fn input_refusal(cause: horsies_core::EnqueueRefusalCause) -> BrokerError {
        BrokerError::EnqueueContract(horsies_core::EnqueueRefusal {
            part: horsies_core::SendPart::Input,
            message: "the input is refused".to_owned(),
            cause,
        })
    }

    /// The class and the `cause` slot of the raw record of a failed
    /// enqueue of `error`.
    fn failure_exception(error: &BrokerError) -> (String, ExceptionRecord) {
        let raw = enqueue_failure_record(&report_command(), error);
        let exception = raw
            .slots
            .iter()
            .find_map(|slot| match &slot.value {
                RawSlotValue::Exception { exception } => Some(exception.clone()),
                RawSlotValue::Text { .. }
                | RawSlotValue::Flag { .. }
                | RawSlotValue::Integer { .. }
                | RawSlotValue::Uuid { .. }
                | RawSlotValue::CodeMember { .. }
                | RawSlotValue::SendPayload { .. } => None,
            })
            .expect("the class has an exception slot");
        (raw.class, exception)
    }

    /// Python: `ValueError(rule)` of `EnqueueCommandV1`.
    #[test]
    fn a_command_rule_refusal_is_validation_failed_with_a_value_error() {
        let error = input_refusal(horsies_core::EnqueueRefusalCause::CommandRuleRefused(
            "retention_class_key must be non-empty".to_owned(),
        ));
        assert_eq!(
            failure_exception(&error),
            (
                "TaskSendValidationFailed".to_owned(),
                ExceptionRecord::ClientClass {
                    class: ClientClassName::ValueError,
                    text: "retention_class_key must be non-empty".to_owned(),
                }
            )
        );
    }

    /// Python: `ValueError(rule)` of `ScopedIdempotencyKey`.
    #[test]
    fn a_key_rule_refusal_is_validation_failed_with_a_value_error() {
        let error = input_refusal(horsies_core::EnqueueRefusalCause::KeyRuleRefused(
            "idempotency key must be non-empty".to_owned(),
        ));
        assert_eq!(
            failure_exception(&error),
            (
                "TaskSendValidationFailed".to_owned(),
                ExceptionRecord::ClientClass {
                    class: ClientClassName::ValueError,
                    text: "idempotency key must be non-empty".to_owned(),
                }
            )
        );
    }

    /// Python: the JSON reader's `SerializationError` with its text.
    #[test]
    fn an_unreadable_json_text_is_validation_failed_with_a_serialization_error() {
        let error = input_refusal(horsies_core::EnqueueRefusalCause::JsonUnreadable(
            "EOF while parsing a value at line 1 column 0".to_owned(),
        ));
        assert_eq!(
            failure_exception(&error),
            (
                "TaskSendValidationFailed".to_owned(),
                ExceptionRecord::ClientClass {
                    class: ClientClassName::SerializationError,
                    text: "EOF while parsing a value at line 1 column 0".to_owned(),
                }
            )
        );
    }

    /// Python: `NoCause()`; the broker found the gone row by a data check.
    #[test]
    fn an_unverifiable_conflict_is_enqueue_failed_with_no_cause() {
        let error = BrokerError::EnqueueConflictUnverifiable {
            task_id: Uuid::from_u128(7),
            task_name: "report".to_owned(),
        };
        assert_eq!(
            failure_exception(&error),
            ("TaskSendEnqueueFailed".to_owned(), ExceptionRecord::NoCause)
        );
    }

    /// Python: the core's class of the error, with its text and its retry
    /// rule, and the operation as its subject.
    #[test]
    fn a_connection_failure_is_enqueue_failed_with_the_core_class() {
        let error = BrokerError::ConnectionFailed("connection refused".to_owned());
        assert_eq!(
            failure_exception(&error),
            (
                "TaskSendEnqueueFailed".to_owned(),
                ExceptionRecord::CoreFailure {
                    cause: CauseRecord::Connection {
                        subject: ENQUEUE_SUBJECT.to_owned(),
                        message: "connection failed: connection refused".to_owned(),
                        retryable: true,
                    }
                }
            )
        );
    }

    /// A task id with another command has no `cause` slot: the record
    /// is the send error's own.
    #[test]
    fn a_payload_mismatch_is_the_record_of_the_send_error() {
        let command = report_command();
        let error = BrokerError::PayloadMismatch {
            task_id: command.task_id,
        };
        assert_eq!(
            enqueue_failure_record(&command, &error),
            RawCatalogError::of(
                ENQUEUE_SUBJECT,
                &send_error_from_enqueue(command.task_id, command.payload.clone(), &error)
            )
        );
    }
}

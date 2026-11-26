//! The send: the rules that each send of an accepted config applies, and the
//! enqueue of one prepared send.
//!
//! - `SendRules.prepareSendFacts` is pure apart from the task id that it
//!   mints: the retention of the send (the Rust client's rule of
//!   `send::options::send_retention`), the payload bound
//!   (`enforce_payload_policy`, which logs the core's warning once per task
//!   name), the task id, and the stored options text with the deadline. A
//!   refused fact is the send error value, not a throw.
//! - `enqueue` is one send through the core, as the Rust client's
//!   `enqueue_once`: the core's checks that need no connection run first
//!   (`CheckedSend::check`, which keeps the shared-class timeout check for a
//!   `REPEAT` send too), then the schema install once per handle, then the
//!   write. Every failure maps to the task send catalog by the core's one
//!   rule (`send_error_from_enqueue`). Writes: a stop of the core in flight
//!   leaves the outcome unknown, and a resend of the same request with
//!   `REPEAT` is safe.

use chrono::{DateTime, Utc};
use horsies_core::core::config::payload::{enforce_payload_policy, PayloadKind};
use horsies_core::core::task::send_types::send_error_from_enqueue;
use horsies_core::telemetry::trace_carrier::SendTrace;
use horsies_core::{
    BrokerError, CheckedSend, EnqueueRetention, FailureCause, PayloadPolicy, RetentionConfig,
    RetentionDays, SendKind as CoreSendKind, TaskOptions, TaskSendError, TaskSendPayload,
};
use napi::bindgen_prelude::{BigInt, Object};
use napi::Env;
use napi_derive::napi;
use uuid::Uuid;

use crate::call::guard::{sync_call, CallKind};
use crate::call::operation::{guarded_start, spawn_operation};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::handle::core_handle::{BrokerConnection, CoreHandle};
use crate::process_core::runtime::pure_call_refusal;
use crate::sends::early_end::thrown_now;
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
/// config and the payload policy. JavaScript gets it from `checkAppConfig`;
/// it has no constructor.
#[napi]
#[derive(Clone, Debug)]
pub struct SendRules {
    retention: RetentionConfig,
    payload: PayloadPolicy,
}

impl SendRules {
    /// The rules of an accepted config. Pure.
    pub fn new(retention: RetentionConfig, payload: PayloadPolicy) -> Self {
        Self { retention, payload }
    }
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

/// The facts of one send that the send rules judge.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct SendFacts {
    pub task_name: String,
    pub queue_name: String,
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
    },
    Refused {
        error: RawCatalogError,
    },
}

#[napi]
impl SendRules {
    /// The prepared facts of one send of a task with the checked `options`.
    /// Mints a task id; the fork and stop rules apply, as for every core
    /// call. Starts nothing.
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
            match self.prepared(&facts, options.options()) {
                Ok(prepared) => Envelope::Value(prepared),
                Err(refusal) => Envelope::Thrown(refusal.into()),
            }
        })
    }
}

impl SendRules {
    /// The prepared send, in the Rust client's order: the retention, the
    /// payload bound, the task id, the options text. A value that breaks a
    /// boundary rule is the refusal. Mints a task id.
    fn prepared(
        &self,
        facts: &SendFacts,
        options: &TaskOptions,
    ) -> Result<PreparedSend, BoundaryRefusal> {
        let deadline = facts.deadline.instant()?;
        let input_bytes = byte_count("inputBytes", facts.input_bytes)?;
        let task_name = facts.task_name.as_str();
        let retention = match self.retention_of(&facts.queue_name, &facts.retention) {
            Ok(retention) => retention,
            Err(cause) => {
                return Ok(refused(&validation_failed(
                    task_name,
                    &format!("Invalid retention: {cause}"),
                )))
            }
        };
        if let Some(too_large) = self.payload_refusal(task_name, input_bytes) {
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
    /// the core's warning above the warn bound. Pure apart from the log.
    fn payload_refusal(&self, task_name: &str, input_bytes: usize) -> Option<TaskSendError> {
        let size =
            enforce_payload_policy(&self.payload, task_name, PayloadKind::Kwargs, input_bytes)?;
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

/// Send one task on `handle`. A request value that breaks a boundary rule
/// resolves with `CLIENT_DEFECT` at once: the package checks each value
/// first.
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
    let command = guarded_start(ENQUEUE_SUBJECT, || {
        command_of(request, send_kind, trace).map_err(NativeThrown::from)
    });
    match command {
        Err(thrown) => thrown_now::<EnqueueOutcome>(env, thrown),
        Ok(command) => {
            let connection = handle.connection();
            spawn_operation(env, ENQUEUE_SUBJECT, CallKind::Write, move |_core| {
                enqueue_operation(connection, command)
            })
        }
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

/// One send, and the outcome as the send catalog names it.
async fn enqueue_operation(
    connection: BrokerConnection,
    command: EnqueueCommand,
) -> EnqueueOutcome {
    match enqueue_once(&connection, &command).await {
        Ok(task_id) => EnqueueOutcome::Enqueued {
            task_id: text_of_uuid(task_id),
        },
        Err(error) => EnqueueOutcome::Failed {
            error: RawCatalogError::of(
                ENQUEUE_SUBJECT,
                &send_error_from_enqueue(command.task_id, command.payload.clone(), &error),
            ),
        },
    }
}

/// The core's check of the send (no connection), the schema install once
/// per handle, then the write. Returns the stored task id.
async fn enqueue_once(
    connection: &BrokerConnection,
    command: &EnqueueCommand,
) -> Result<Uuid, BrokerError> {
    let handle = connection.lazy_handle();
    let payload = &command.payload;
    let send = CheckedSend::check(
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
    .with_trace(command.trace.clone());
    let broker = handle.initialized_broker().await?;
    broker.enqueue_checked(&send, command.send_kind).await
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use horsies_core::catalog::CodeEnum;

    use super::*;
    use crate::values::boundary::bigint_of_i64;
    use crate::values::catalog_values::RawSlotValue;

    fn rules() -> SendRules {
        let mut retention = RetentionConfig::default();
        retention.retention_periods = vec![chrono::Duration::days(7)];
        let _previous = retention.queue_retention.insert("audit".to_owned(), None);
        SendRules::new(
            retention,
            PayloadPolicy {
                warn_bytes: None,
                reject_bytes: Some(100),
            },
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
            task_name: "report".to_owned(),
            queue_name: "default".to_owned(),
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
            .prepared(&facts(RetentionChoice::QueueOrDefault, 10.0), &options())
            .expect("the facts cross");
        assert_eq!(
            prepared_retention(prepared),
            RetentionRecord::ClassKey {
                key: "standard_30d".to_owned()
            }
        );
        let audit = SendFacts {
            queue_name: "audit".to_owned(),
            ..facts(RetentionChoice::QueueOrDefault, 10.0)
        };
        let prepared = rules()
            .prepared(&audit, &options())
            .expect("the facts cross");
        assert_eq!(prepared_retention(prepared), RetentionRecord::Forever);
    }

    #[test]
    fn a_registered_period_is_its_class_and_another_period_is_a_validation_failure() {
        let prepared = rules()
            .prepared(
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
                &facts(RetentionChoice::Days { days: 9.0 }, 10.0),
                &options(),
            )
            .expect("the facts cross");
        assert_eq!(refused_class(refused), "TaskSendValidationFailed");
    }

    #[test]
    fn an_input_above_the_reject_bound_is_payload_too_large_with_its_sizes() {
        let refused = rules()
            .prepared(&facts(RetentionChoice::Forever, 101.0), &options())
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
            .prepared(&deadline, &options())
            .expect("the facts cross")
        else {
            panic!("the send is prepared");
        };
        assert!(
            task_options.contains(r#""good_until":"2026-08-12T09:10:12.654321+00:00""#),
            "{task_options}"
        );
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
}

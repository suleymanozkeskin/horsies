//! The task reads on a core handle: the result read and wait, the task info
//! read, and the role of this process.
//!
//! - In a task child each read goes through the child host's producer
//!   handle, else through the given handle (`ProducerHandle`), as the Rust
//!   client's reads do.
//! - Each read installs the schema once per handle first, as the Python
//!   bridge's reads do; a failed connect or install is
//!   `TASK_INFO_QUERY_FAILED` (`DatabaseOperationFailed`).
//! - A failed read is a raw value of the broker catalog: `DatabaseOperationFailed`
//!   (`TASK_INFO_QUERY_FAILED`), `DatabaseInvalidInput` or
//!   `DatabaseInvalidJsonPayload`, as the core gives it. A wait inside a task
//!   is refused by the package before the call (`processRole`), so the read
//!   gives no `DatabaseWaitInsideTask`.
//! - A found task has its stored result by the stage of the task when the
//!   read ended: a stored envelope, a wait that ended before the task
//!   finished, or a finished task that stores no result (a cancel before a
//!   start, an expiry, a terminal row of the live table).
//! - Reads change nothing: a stop of the core in flight leaves no change.
//! - The package refuses a wait outside 0 to `MAX_WAIT_LIMIT_MS` before the
//!   call, so such a value here is a defect of the package.

use std::time::Duration;

use chrono::{DateTime, Utc};
use horsies_core::{
    BrokerError, BrokerOperationSource, DatabaseOperationError, Execution, ExecutionClass,
    ProcessRole, RawResultRead, RawResultRecord, StoredEnvelopeText, TaskAbsenceReason,
    TaskAttemptInfo, TaskAttemptOutcome, TaskInfo, TaskInfoRead as CoreTaskInfoRead, TaskStatus,
};
use napi::bindgen_prelude::{BigInt, Object};
use napi::Env;
use napi_derive::napi;
use uuid::Uuid;

use crate::call::guard::{sync_call, CallKind};
use crate::call::operation::{guarded_start, spawn_operation};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::handle::core_handle::CoreHandle;
use crate::sends::early_end::thrown_now;
use crate::sends::producer_handle::ProducerHandle;
use crate::values::boundary::{epoch_micros_of, text_of_uuid, u32_of_number, uuid_of_text};
use crate::values::catalog_values::RawCatalogError;

/// The subject of the exception of a failed result read.
const RESULT_SUBJECT: &str = "raw_result_record";

/// The subject of the exception of a failed task info read.
const INFO_SUBJECT: &str = "task_info";

/// How long a result read waits for a task that has not finished.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum WaitBound {
    UntilEnd,
    UpTo { milliseconds: f64 },
}

/// The status of a task.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskStatusName {
    Pending,
    Claimed,
    Running,
    Completed,
    Failed,
    Cancelled,
    Expired,
}

impl TaskStatusName {
    /// The name of the core's status. Pure.
    fn of(status: TaskStatus) -> Self {
        match status {
            TaskStatus::Pending => Self::Pending,
            TaskStatus::Claimed => Self::Claimed,
            TaskStatus::Running => Self::Running,
            TaskStatus::Completed => Self::Completed,
            TaskStatus::Failed => Self::Failed,
            TaskStatus::Cancelled => Self::Cancelled,
            TaskStatus::Expired => Self::Expired,
        }
    }
}

/// Why neither the live table nor the task history holds a task.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AbsenceReason {
    PredatesRetainedHistory,
    WithinRetainedHistory,
    NotClassified,
}

impl AbsenceReason {
    /// The name of the core's reason. Pure.
    fn of(reason: TaskAbsenceReason) -> Self {
        match reason {
            TaskAbsenceReason::PredatesRetainedHistory => Self::PredatesRetainedHistory,
            TaskAbsenceReason::WithinRetainedHistory => Self::WithinRetainedHistory,
            TaskAbsenceReason::NotClassified => Self::NotClassified,
        }
    }
}

/// The status of a task that has not finished.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LiveStatusName {
    Pending,
    Claimed,
    Running,
}

/// The status of a finished task.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FinishedStatusName {
    Completed,
    Failed,
    Cancelled,
    Expired,
}

/// The stage of a status: the task has not finished, or it has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatusStage {
    Live(LiveStatusName),
    Finished(FinishedStatusName),
}

impl StatusStage {
    /// The stage of the core's status. Pure.
    fn of(status: TaskStatus) -> Self {
        match status {
            TaskStatus::Pending => Self::Live(LiveStatusName::Pending),
            TaskStatus::Claimed => Self::Live(LiveStatusName::Claimed),
            TaskStatus::Running => Self::Live(LiveStatusName::Running),
            TaskStatus::Completed => Self::Finished(FinishedStatusName::Completed),
            TaskStatus::Failed => Self::Finished(FinishedStatusName::Failed),
            TaskStatus::Cancelled => Self::Finished(FinishedStatusName::Cancelled),
            TaskStatus::Expired => Self::Finished(FinishedStatusName::Expired),
        }
    }
}

/// The stored result of a task when the read ended, with the status of the
/// task:
/// - `Stored`: the row stores this result envelope text;
/// - `WaitEnded`: the wait ended before the task finished, so no result is
///   stored yet;
/// - `FinishedWithoutResult`: the task finished and stores no result (a
///   cancel before a start, an expiry, a terminal row of the live table).
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvelopeFact {
    Stored {
        status: TaskStatusName,
        text: String,
    },
    WaitEnded {
        status: LiveStatusName,
    },
    FinishedWithoutResult {
        status: FinishedStatusName,
    },
}

impl EnvelopeFact {
    /// The fact of the core's status and stored text: the text as the row
    /// stores it, so an integer outside the `i64` and `u64` range keeps
    /// every digit. Pure.
    fn of(status: TaskStatus, raw_result: Option<StoredEnvelopeText>) -> Self {
        match (raw_result, StatusStage::of(status)) {
            (Some(envelope), _) => Self::Stored {
                status: TaskStatusName::of(status),
                text: envelope.as_str().to_owned(),
            },
            (None, StatusStage::Live(status)) => Self::WaitEnded { status },
            (None, StatusStage::Finished(status)) => Self::FinishedWithoutResult { status },
        }
    }
}

/// One task and its stored result.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultRecord {
    pub task_id: String,
    pub task_name: String,
    pub envelope: EnvelopeFact,
}

impl ResultRecord {
    /// The record of the core's read. Pure.
    fn of(record: RawResultRecord) -> Self {
        Self {
            task_id: text_of_uuid(record.task_id),
            task_name: record.task_name,
            envelope: EnvelopeFact::of(record.status, record.raw_result),
        }
    }
}

/// The outcome of one result read.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultRead {
    Found { record: ResultRecord },
    Absent { reason: AbsenceReason },
    Failed { error: RawCatalogError },
}

/// Read the result of one task, waiting as `wait` says while it has not
/// finished: in a task child through the child host's producer handle,
/// else through `handle`. A task id or a wait that breaks a boundary rule
/// resolves with `CLIENT_DEFECT` at once.
#[napi(ts_return_type = "Promise<NativeEnvelope<ResultRead>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn get_result<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    task_id: String,
    wait: WaitBound,
) -> napi::Result<Object<'env>> {
    result_read_on(env, ProducerHandle::of_process(handle), &task_id, &wait)
}

/// Read the result of one task through `producer`, as `getResult` does.
///
/// Failure exits: the `napi::Error` of a Promise that Node-API cannot
/// create. Every other end is an envelope.
pub fn result_read_on<'env>(
    env: &'env Env,
    producer: ProducerHandle,
    task_id: &str,
    wait: &WaitBound,
) -> napi::Result<Object<'env>> {
    let request = guarded_start(RESULT_SUBJECT, || {
        let task_id = uuid_of_text("taskId", task_id)?;
        let limit = match wait {
            WaitBound::UntilEnd => None,
            WaitBound::UpTo { milliseconds } => Some(Duration::from_millis(u64::from(
                u32_of_number("wait.milliseconds", *milliseconds)?,
            ))),
        };
        Ok::<_, NativeThrown>((task_id, limit))
    });
    match request {
        Err(thrown) => thrown_now::<ResultRead>(env, thrown),
        Ok((task_id, limit)) => {
            spawn_operation(env, RESULT_SUBJECT, CallKind::Read, move |_core| {
                result_operation(producer, task_id, limit)
            })
        }
    }
}

/// One result read through `producer`. Installs the schema once per handle
/// first.
async fn result_operation(
    producer: ProducerHandle,
    task_id: Uuid,
    limit: Option<Duration>,
) -> ResultRead {
    match producer.lazy_handle().initialized_broker().await {
        Ok(broker) => result_read_of(broker.get_raw_result_record(task_id, limit).await),
        Err(error) => result_install_failed(task_id, error),
    }
}

/// The outcome of a result read whose connect or schema install failed:
/// `DatabaseOperationFailed` (`TASK_INFO_QUERY_FAILED`) with the cause. Pure.
fn result_install_failed(task_id: Uuid, error: BrokerError) -> ResultRead {
    ResultRead::Failed {
        error: RawCatalogError::of(
            RESULT_SUBJECT,
            &DatabaseOperationError::raw_result_query_failed(task_id, error),
        ),
    }
}

/// The outcome of a core result read. Pure.
fn result_read_of(read: Result<RawResultRead, DatabaseOperationError>) -> ResultRead {
    match read {
        Ok(RawResultRead::Found(record)) => ResultRead::Found {
            record: ResultRecord::of(record),
        },
        Ok(RawResultRead::Absent { reason }) => ResultRead::Absent {
            reason: AbsenceReason::of(reason),
        },
        Err(error) => ResultRead::Failed {
            error: RawCatalogError::of(RESULT_SUBJECT, &error),
        },
    }
}

/// A text fact of a task row that the row may not hold.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordedText {
    NotRecorded,
    Recorded { text: String },
}

impl RecordedText {
    /// The fact of a text that a row may not hold. Pure.
    pub fn of(text: Option<String>) -> Self {
        match text {
            None => Self::NotRecorded,
            Some(text) => Self::Recorded { text },
        }
    }
}

/// An instant of a task row that the row may not hold, in epoch
/// microseconds.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordedInstant {
    NotRecorded,
    Recorded { epoch_micros: BigInt },
}

impl RecordedInstant {
    /// The fact of an instant that a row may not hold. Pure.
    pub fn of(instant: Option<DateTime<Utc>>) -> Self {
        match instant {
            None => Self::NotRecorded,
            Some(instant) => Self::Recorded {
                epoch_micros: epoch_micros_of(instant),
            },
        }
    }
}

/// A count of a task row that the row may not hold.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum RecordedCount {
    NotRecorded,
    Recorded { value: f64 },
}

impl RecordedCount {
    fn of(count: Option<f64>) -> Self {
        match count {
            None => Self::NotRecorded,
            Some(value) => Self::Recorded { value },
        }
    }
}

/// How a worker runs a task: the class of a live task; a task read from
/// history does not keep it.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionRecord {
    Isolated,
    SharedAsync,
    SharedThread,
    NotKeptInHistory,
}

impl ExecutionRecord {
    fn of(class: ExecutionClass) -> Self {
        match class {
            ExecutionClass::Live(Execution::Isolated) => Self::Isolated,
            ExecutionClass::Live(Execution::SharedAsync) => Self::SharedAsync,
            ExecutionClass::Live(Execution::SharedThread) => Self::SharedThread,
            ExecutionClass::NotKeptInHistory => Self::NotKeptInHistory,
        }
    }
}

/// The outcome of one attempt of a task.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptOutcomeName {
    Completed,
    Failed,
    WorkerFailure,
    Cancelled,
}

impl AttemptOutcomeName {
    fn of(outcome: TaskAttemptOutcome) -> Self {
        match outcome {
            TaskAttemptOutcome::Completed => Self::Completed,
            TaskAttemptOutcome::Failed => Self::Failed,
            TaskAttemptOutcome::WorkerFailure => Self::WorkerFailure,
            TaskAttemptOutcome::Cancelled => Self::Cancelled,
        }
    }
}

/// One attempt of a task.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct TaskAttemptRecord {
    pub attempt: f64,
    pub outcome: AttemptOutcomeName,
    pub will_retry: bool,
    pub started_at_micros: BigInt,
    pub finished_at_micros: BigInt,
    pub error_code: RecordedText,
    pub error_message: RecordedText,
    pub failed_reason: RecordedText,
    pub worker_id: RecordedText,
    pub worker_hostname: RecordedText,
    pub worker_pid: RecordedCount,
    pub worker_process_name: RecordedText,
}

impl TaskAttemptRecord {
    fn of(attempt: TaskAttemptInfo) -> Self {
        Self {
            attempt: f64::from(attempt.attempt),
            outcome: AttemptOutcomeName::of(attempt.outcome),
            will_retry: attempt.will_retry,
            started_at_micros: epoch_micros_of(attempt.started_at),
            finished_at_micros: epoch_micros_of(attempt.finished_at),
            error_code: RecordedText::of(attempt.error_code),
            error_message: RecordedText::of(attempt.error_message),
            failed_reason: RecordedText::of(attempt.failed_reason),
            worker_id: RecordedText::of(attempt.worker_id),
            worker_hostname: RecordedText::of(attempt.worker_hostname),
            worker_pid: RecordedCount::of(attempt.worker_pid.map(f64::from)),
            worker_process_name: RecordedText::of(attempt.worker_process_name),
        }
    }
}

/// The info of one task: its status, times, failed reason and every
/// attempt, newest first. No result.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct TaskInfoRecord {
    pub task_id: String,
    pub task_name: String,
    pub status: TaskStatusName,
    pub queue: String,
    pub priority: f64,
    pub retry_count: f64,
    pub max_retries: f64,
    pub next_retry_at: RecordedInstant,
    pub sent_at: RecordedInstant,
    pub due_at_micros: BigInt,
    pub claimed_at: RecordedInstant,
    pub started_at: RecordedInstant,
    pub completed_at: RecordedInstant,
    pub failed_at: RecordedInstant,
    pub worker_hostname: RecordedText,
    pub worker_pid: RecordedCount,
    pub worker_process_name: RecordedText,
    pub error_code: RecordedText,
    pub failed_reason: RecordedText,
    pub attempts: Vec<TaskAttemptRecord>,
    pub execution: ExecutionRecord,
}

impl TaskInfoRecord {
    /// The record of the core's info. The read asks for the attempts, so
    /// the core always gives a list (empty when no attempt is recorded).
    /// Pure.
    fn of(info: TaskInfo<StoredEnvelopeText>) -> Self {
        let attempts = info
            .attempts
            .expect("the task info read asks for the attempts, so the core gives a list");
        Self {
            task_id: text_of_uuid(info.task_id),
            task_name: info.task_name,
            status: TaskStatusName::of(info.status),
            queue: info.queue,
            priority: f64::from(info.priority),
            retry_count: f64::from(info.retry_count),
            max_retries: f64::from(info.max_retries),
            next_retry_at: RecordedInstant::of(info.next_retry_at),
            sent_at: RecordedInstant::of(info.sent_at),
            due_at_micros: epoch_micros_of(info.due_at),
            claimed_at: RecordedInstant::of(info.claimed_at),
            started_at: RecordedInstant::of(info.started_at),
            completed_at: RecordedInstant::of(info.completed_at),
            failed_at: RecordedInstant::of(info.failed_at),
            worker_hostname: RecordedText::of(info.worker_hostname),
            worker_pid: RecordedCount::of(info.worker_pid.map(f64::from)),
            worker_process_name: RecordedText::of(info.worker_process_name),
            error_code: RecordedText::of(info.error_code),
            failed_reason: RecordedText::of(info.failed_reason),
            attempts: attempts.into_iter().map(TaskAttemptRecord::of).collect(),
            execution: ExecutionRecord::of(info.execution),
        }
    }
}

/// The outcome of one task info read.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq)]
#[allow(
    clippy::large_enum_variant,
    reason = "the value becomes a JavaScript object when the call returns, and napi-rs has no conversion of a Box"
)]
pub enum TaskInfoRead {
    Found { record: TaskInfoRecord },
    Absent { reason: AbsenceReason },
    Failed { error: RawCatalogError },
}

/// Read the info of one task: status, times, the failed reason and every
/// attempt, newest first; no result. In a task child the read goes through
/// the child host's producer handle, else through `handle`. A task id that
/// breaks a boundary rule resolves with `CLIENT_DEFECT` at once.
#[napi(ts_return_type = "Promise<NativeEnvelope<TaskInfoRead>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn task_info<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    task_id: String,
) -> napi::Result<Object<'env>> {
    task_info_on(env, ProducerHandle::of_process(handle), &task_id)
}

/// Read the info of one task through `producer`, as `taskInfo` does.
///
/// Failure exits: the `napi::Error` of a Promise that Node-API cannot
/// create. Every other end is an envelope.
pub fn task_info_on<'env>(
    env: &'env Env,
    producer: ProducerHandle,
    task_id: &str,
) -> napi::Result<Object<'env>> {
    let task_id = guarded_start(INFO_SUBJECT, || {
        uuid_of_text("taskId", task_id).map_err(NativeThrown::from)
    });
    match task_id {
        Err(thrown) => thrown_now::<TaskInfoRead>(env, thrown),
        Ok(task_id) => spawn_operation(env, INFO_SUBJECT, CallKind::Read, move |_core| {
            task_info_operation(producer, task_id)
        }),
    }
}

/// One task info read through `producer`. Installs the schema once per
/// handle first.
async fn task_info_operation(producer: ProducerHandle, task_id: Uuid) -> TaskInfoRead {
    match producer.lazy_handle().initialized_broker().await {
        Ok(broker) => task_info_read_of(
            broker
                .get_task_info_with_attempts(task_id, false, true, true)
                .await,
        ),
        Err(error) => task_info_install_failed(task_id, error),
    }
}

/// The outcome of a task info read whose connect or schema install failed:
/// `DatabaseOperationFailed` (`TASK_INFO_QUERY_FAILED`) with the cause. Pure.
fn task_info_install_failed(task_id: Uuid, error: BrokerError) -> TaskInfoRead {
    TaskInfoRead::Failed {
        error: RawCatalogError::of(
            INFO_SUBJECT,
            &DatabaseOperationError::task_info_query_failed(
                task_id,
                BrokerOperationSource::Broker(error),
            ),
        ),
    }
}

/// The outcome of a core task info read. Pure.
fn task_info_read_of(read: Result<CoreTaskInfoRead, DatabaseOperationError>) -> TaskInfoRead {
    match read {
        Ok(CoreTaskInfoRead::Found(info)) => TaskInfoRead::Found {
            record: TaskInfoRecord::of(*info),
        },
        Ok(CoreTaskInfoRead::Absent { reason }) => TaskInfoRead::Absent {
            reason: AbsenceReason::of(reason),
        },
        Err(error) => TaskInfoRead::Failed {
            error: RawCatalogError::of(INFO_SUBJECT, &error),
        },
    }
}

/// Whether this process runs task bodies.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessRoleName {
    /// A task child.
    RunsTaskBodies,
    /// A producer, a worker parent, a scheduler or a handler child.
    RunsNoTaskBodies,
}

/// The role of this process: a pure read that starts nothing.
#[napi(ts_return_type = "NativeEnvelope<ProcessRoleName>")]
pub fn process_role() -> Envelope<ProcessRoleName> {
    sync_call("process_role", || {
        Envelope::Value(match horsies_core::process_role() {
            ProcessRole::RunsTaskBodies => ProcessRoleName::RunsTaskBodies,
            ProcessRole::RunsNoTaskBodies => ProcessRoleName::RunsNoTaskBodies,
        })
    })
}

#[cfg(test)]
mod tests {
    use horsies_core::{BrokerCheckCause, ResultSlot};

    use super::*;
    use crate::sends::task_options::ExecutionName;
    use crate::values::catalog_values::RawSlotValue;

    fn task() -> Uuid {
        Uuid::from_u128(7)
    }

    fn found(status: TaskStatus, raw_result: Option<StoredEnvelopeText>) -> ResultRead {
        result_read_of(Ok(RawResultRead::Found(RawResultRecord {
            task_id: task(),
            task_name: "add".to_owned(),
            status,
            raw_result,
        })))
    }

    fn envelope_of(read: ResultRead) -> EnvelopeFact {
        match read {
            ResultRead::Found { record } => record.envelope,
            ResultRead::Absent { reason } => panic!("the task is found, got {reason:?}"),
            ResultRead::Failed { error } => panic!("the task is found, got {error:?}"),
        }
    }

    #[test]
    fn a_terminal_task_gives_its_status_and_envelope_text() {
        let envelope = StoredEnvelopeText::parse(r#"{"ok":18446744073709551616}"#.to_owned())
            .expect("the text is strict JSON")
            .expect("an object is an envelope text");
        assert_eq!(
            found(TaskStatus::Completed, Some(envelope)),
            ResultRead::Found {
                record: ResultRecord {
                    task_id: text_of_uuid(task()),
                    task_name: "add".to_owned(),
                    envelope: EnvelopeFact::Stored {
                        status: TaskStatusName::Completed,
                        text: r#"{"ok":18446744073709551616}"#.to_owned()
                    },
                }
            }
        );
    }

    #[test]
    fn a_wait_that_ended_first_gives_the_live_status_and_no_envelope() {
        for (status, live) in [
            (TaskStatus::Pending, LiveStatusName::Pending),
            (TaskStatus::Claimed, LiveStatusName::Claimed),
            (TaskStatus::Running, LiveStatusName::Running),
        ] {
            assert_eq!(
                envelope_of(found(status, None)),
                EnvelopeFact::WaitEnded { status: live }
            );
        }
    }

    /// A terminal task with no stored result: a cancel before a start, an
    /// expiry, or a terminal row of the live table. The wait did not end
    /// early, so this is not a wait that ended.
    #[test]
    fn a_terminal_task_with_no_envelope_is_finished_without_result() {
        for (status, finished) in [
            (TaskStatus::Completed, FinishedStatusName::Completed),
            (TaskStatus::Failed, FinishedStatusName::Failed),
            (TaskStatus::Cancelled, FinishedStatusName::Cancelled),
            (TaskStatus::Expired, FinishedStatusName::Expired),
        ] {
            assert_eq!(
                envelope_of(found(status, None)),
                EnvelopeFact::FinishedWithoutResult { status: finished }
            );
        }
    }

    /// The code member and the retry flag of a raw `DatabaseOperationFailed`.
    fn operation_failed_slots(error: &RawCatalogError) -> (Option<String>, Option<bool>) {
        let code = error.slots.iter().find_map(|slot| match &slot.value {
            RawSlotValue::CodeMember { value, .. } => Some(value.clone()),
            RawSlotValue::Text { .. }
            | RawSlotValue::Flag { .. }
            | RawSlotValue::Integer { .. }
            | RawSlotValue::Uuid { .. }
            | RawSlotValue::SendPayload { .. }
            | RawSlotValue::Exception { .. } => None,
        });
        let retryable = error.slots.iter().find_map(|slot| match &slot.value {
            RawSlotValue::Flag { value } => Some(*value),
            RawSlotValue::Text { .. }
            | RawSlotValue::CodeMember { .. }
            | RawSlotValue::Integer { .. }
            | RawSlotValue::Uuid { .. }
            | RawSlotValue::SendPayload { .. }
            | RawSlotValue::Exception { .. } => None,
        });
        (code, retryable)
    }

    /// A connect or a schema install that fails (here: the listener closed)
    /// is `TASK_INFO_QUERY_FAILED`, retryable, for both reads.
    #[test]
    fn a_failed_connect_or_install_of_a_read_is_task_info_query_failed() {
        let ResultRead::Failed { error } =
            result_install_failed(task(), BrokerError::ListenerClosed)
        else {
            panic!("a failed install is a catalog value");
        };
        assert_eq!(error.class, "DatabaseOperationFailed");
        assert_eq!(
            operation_failed_slots(&error),
            (Some("TASK_INFO_QUERY_FAILED".to_owned()), Some(true))
        );
        let TaskInfoRead::Failed { error } =
            task_info_install_failed(task(), BrokerError::ListenerClosed)
        else {
            panic!("a failed install is a catalog value");
        };
        assert_eq!(error.class, "DatabaseOperationFailed");
        assert_eq!(
            operation_failed_slots(&error),
            (Some("TASK_INFO_QUERY_FAILED".to_owned()), Some(true))
        );
    }

    #[test]
    fn a_task_info_maps_each_fact_and_each_attempt() {
        let due_at =
            DateTime::<Utc>::from_timestamp(1_786_525_812, 0).expect("the instant is in range");
        let started_at = due_at + chrono::Duration::seconds(5);
        let info = TaskInfo::<StoredEnvelopeText> {
            task_id: task(),
            task_name: "charge".to_owned(),
            status: TaskStatus::Failed,
            queue: "billing".to_owned(),
            priority: 7,
            retry_count: 1,
            max_retries: 3,
            next_retry_at: None,
            sent_at: Some(due_at),
            due_at,
            claimed_at: None,
            started_at: Some(started_at),
            completed_at: None,
            failed_at: Some(started_at),
            worker_hostname: Some("host-1".to_owned()),
            worker_pid: Some(4_242),
            worker_process_name: None,
            error_code: Some("CARD_DECLINED".to_owned()),
            failed_reason: None,
            result: ResultSlot::NotRequested,
            attempts: Some(vec![TaskAttemptInfo {
                task_id: task(),
                attempt: 1,
                outcome: TaskAttemptOutcome::Failed,
                will_retry: true,
                started_at,
                finished_at: started_at,
                error_code: Some("CARD_DECLINED".to_owned()),
                error_message: None,
                failed_reason: None,
                worker_id: None,
                worker_hostname: None,
                worker_pid: Some(4_242),
                worker_process_name: None,
            }]),
            execution: ExecutionClass::Live(Execution::Isolated),
        };
        let TaskInfoRead::Found { record } =
            task_info_read_of(Ok(CoreTaskInfoRead::Found(Box::new(info))))
        else {
            panic!("the info is found");
        };
        assert_eq!(record.task_id, text_of_uuid(task()));
        assert_eq!(record.status, TaskStatusName::Failed);
        assert_eq!(record.queue, "billing");
        assert_eq!(record.priority, 7.0);
        assert_eq!(record.next_retry_at, RecordedInstant::NotRecorded);
        assert_eq!(
            record.sent_at,
            RecordedInstant::Recorded {
                epoch_micros: epoch_micros_of(due_at)
            }
        );
        assert_eq!(
            record.worker_pid,
            RecordedCount::Recorded { value: 4_242.0 }
        );
        assert_eq!(record.worker_process_name, RecordedText::NotRecorded);
        assert_eq!(
            record.error_code,
            RecordedText::Recorded {
                text: "CARD_DECLINED".to_owned()
            }
        );
        assert_eq!(record.execution, ExecutionRecord::Isolated);
        let [attempt] = record.attempts.as_slice() else {
            panic!("the info has one attempt");
        };
        assert_eq!(attempt.outcome, AttemptOutcomeName::Failed);
        assert!(attempt.will_retry);
        assert_eq!(attempt.error_message, RecordedText::NotRecorded);
    }

    #[test]
    fn a_task_held_nowhere_is_absent_with_its_reason() {
        assert_eq!(
            result_read_of(Ok(RawResultRead::Absent {
                reason: TaskAbsenceReason::PredatesRetainedHistory
            })),
            ResultRead::Absent {
                reason: AbsenceReason::PredatesRetainedHistory
            }
        );
    }

    #[test]
    fn a_failed_read_is_a_raw_value_of_the_broker_catalog() {
        let read = result_read_of(Err(DatabaseOperationError::InvalidJsonPayload {
            message: "digest mismatch".to_owned(),
            cause: Box::new(BrokerCheckCause::Check),
        }));
        let ResultRead::Failed { error } = read else {
            panic!("a failed read is a catalog value");
        };
        assert_eq!(error.class, "DatabaseInvalidJsonPayload");
    }

    #[test]
    fn each_status_and_reason_has_its_name() {
        assert_eq!(
            TaskStatusName::of(TaskStatus::Expired),
            TaskStatusName::Expired
        );
        assert_eq!(
            AbsenceReason::of(TaskAbsenceReason::NotClassified),
            AbsenceReason::NotClassified
        );
        assert_eq!(
            ExecutionRecord::of(ExecutionClass::NotKeptInHistory),
            ExecutionRecord::NotKeptInHistory
        );
        assert_eq!(
            ExecutionRecord::of(ExecutionClass::Live(Execution::SharedThread)),
            ExecutionRecord::SharedThread
        );
    }

    #[test]
    fn the_role_of_a_test_process_runs_no_task_bodies() {
        assert_eq!(
            process_role(),
            Envelope::Value(ProcessRoleName::RunsNoTaskBodies)
        );
    }

    #[test]
    fn the_execution_names_of_a_live_task_match_the_check_names() {
        for (name, record) in [
            (ExecutionName::Isolated, ExecutionRecord::Isolated),
            (ExecutionName::SharedAsync, ExecutionRecord::SharedAsync),
            (ExecutionName::SharedThread, ExecutionRecord::SharedThread),
        ] {
            assert_eq!(
                ExecutionRecord::of(ExecutionClass::Live(name.execution())),
                record
            );
        }
    }
}

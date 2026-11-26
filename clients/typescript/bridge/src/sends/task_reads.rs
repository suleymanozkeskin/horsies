//! The task reads on a core handle: the result read and wait, the task info
//! read, and the role of this process.
//!
//! - Each read installs the schema once per handle first, as the Python
//!   bridge's reads do; a failed connect or install is
//!   `TASK_INFO_QUERY_FAILED` (`BrokerOperationFailed`).
//! - A failed read is a raw value of the broker catalog: `BrokerOperationFailed`
//!   (`TASK_INFO_QUERY_FAILED`), `BrokerInvalidInput`,
//!   `BrokerInvalidJsonPayload` or `BrokerWaitInsideTask`, as the core gives
//!   it.
//! - Reads change nothing: a stop of the core in flight leaves no change.
//! - The package refuses a wait outside 0 to `MAX_WAIT_LIMIT_MS` before the
//!   call, so such a value here is a defect of the package.

use std::time::Duration;

use chrono::{DateTime, Utc};
use horsies_core::{
    BrokerOperationError, BrokerOperationSource, Execution, ExecutionClass, ProcessRole,
    RawResultRead, RawResultRecord, StoredEnvelopeText, TaskAbsenceReason, TaskAttemptInfo,
    TaskAttemptOutcome, TaskInfo, TaskInfoRead as CoreTaskInfoRead, TaskStatus,
};
use napi::bindgen_prelude::{BigInt, Object};
use napi::Env;
use napi_derive::napi;
use uuid::Uuid;

use crate::call::guard::{sync_call, CallKind};
use crate::call::operation::{guarded_start, spawn_operation};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::handle::core_handle::{BrokerConnection, CoreHandle};
use crate::sends::early_end::thrown_now;
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

/// The stored result envelope of a task: its text, or none when the wait
/// ended before the task.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvelopeFact {
    Stored { text: String },
    NotFinished,
}

/// The status of one task and its stored result envelope.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultRecord {
    pub task_id: String,
    pub task_name: String,
    pub status: TaskStatusName,
    pub envelope: EnvelopeFact,
}

impl ResultRecord {
    /// The record of the core's read: the envelope text as the row stores
    /// it, so an integer outside the `i64` and `u64` range keeps every
    /// digit. Pure.
    fn of(record: RawResultRecord) -> Self {
        Self {
            task_id: text_of_uuid(record.task_id),
            task_name: record.task_name,
            status: TaskStatusName::of(record.status),
            envelope: match record.raw_result {
                Some(envelope) => EnvelopeFact::Stored {
                    text: envelope.as_str().to_owned(),
                },
                None => EnvelopeFact::NotFinished,
            },
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
/// finished. A task id or a wait that breaks a boundary rule resolves with
/// `CLIENT_DEFECT` at once.
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
    let request = guarded_start(RESULT_SUBJECT, || {
        let task_id = uuid_of_text("taskId", &task_id)?;
        let limit = match wait {
            WaitBound::UntilEnd => None,
            WaitBound::UpTo { milliseconds } => Some(Duration::from_millis(u64::from(
                u32_of_number("wait.milliseconds", milliseconds)?,
            ))),
        };
        Ok::<_, NativeThrown>((task_id, limit))
    });
    match request {
        Err(thrown) => thrown_now::<ResultRead>(env, thrown),
        Ok((task_id, limit)) => {
            let connection = handle.connection();
            spawn_operation(env, RESULT_SUBJECT, CallKind::Read, move |_core| {
                result_operation(connection, task_id, limit)
            })
        }
    }
}

/// One result read. Installs the schema once per handle first.
async fn result_operation(
    connection: BrokerConnection,
    task_id: Uuid,
    limit: Option<Duration>,
) -> ResultRead {
    let broker = match connection.lazy_handle().initialized_broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return ResultRead::Failed {
                error: RawCatalogError::of(
                    RESULT_SUBJECT,
                    &BrokerOperationError::raw_result_query_failed(task_id, error),
                ),
            }
        }
    };
    result_read_of(broker.get_raw_result_record(task_id, limit).await)
}

/// The outcome of a core result read. Pure.
fn result_read_of(read: Result<RawResultRead, BrokerOperationError>) -> ResultRead {
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
    fn of(text: Option<String>) -> Self {
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
    fn of(instant: Option<DateTime<Utc>>) -> Self {
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
    pub queue_name: String,
    pub priority: f64,
    pub retry_count: f64,
    pub max_retries: f64,
    pub next_retry_at: RecordedInstant,
    pub sent_at: RecordedInstant,
    pub enqueued_at_micros: BigInt,
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
    fn of(info: TaskInfo<StoredEnvelopeText>) -> Self {
        let attempts = info
            .attempts
            .expect("the task info read asks for the attempts, so the core gives a list");
        Self {
            task_id: text_of_uuid(info.task_id),
            task_name: info.task_name,
            status: TaskStatusName::of(info.status),
            queue_name: info.queue_name,
            priority: f64::from(info.priority),
            retry_count: f64::from(info.retry_count),
            max_retries: f64::from(info.max_retries),
            next_retry_at: RecordedInstant::of(info.next_retry_at),
            sent_at: RecordedInstant::of(info.sent_at),
            enqueued_at_micros: epoch_micros_of(info.enqueued_at),
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
pub enum TaskInfoRead {
    Found { record: TaskInfoRecord },
    Absent { reason: AbsenceReason },
    Failed { error: RawCatalogError },
}

/// Read the info of one task: status, times, the failed reason and every
/// attempt, newest first; no result. A task id that breaks a boundary rule
/// resolves with `CLIENT_DEFECT` at once.
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
    let task_id = guarded_start(INFO_SUBJECT, || {
        uuid_of_text("taskId", &task_id).map_err(NativeThrown::from)
    });
    match task_id {
        Err(thrown) => thrown_now::<TaskInfoRead>(env, thrown),
        Ok(task_id) => {
            let connection = handle.connection();
            spawn_operation(env, INFO_SUBJECT, CallKind::Read, move |_core| {
                task_info_operation(connection, task_id)
            })
        }
    }
}

/// One task info read. Installs the schema once per handle first.
async fn task_info_operation(connection: BrokerConnection, task_id: Uuid) -> TaskInfoRead {
    let broker = match connection.lazy_handle().initialized_broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return TaskInfoRead::Failed {
                error: RawCatalogError::of(
                    INFO_SUBJECT,
                    &BrokerOperationError::task_info_query_failed(
                        task_id,
                        BrokerOperationSource::Broker(error),
                    ),
                ),
            }
        }
    };
    let read = broker
        .get_task_info_with_attempts(task_id, false, true, true)
        .await;
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
    use horsies_core::BrokerCheckCause;

    use super::*;
    use crate::sends::task_options::ExecutionName;

    fn task() -> Uuid {
        Uuid::from_u128(7)
    }

    #[test]
    fn a_terminal_task_gives_its_status_and_envelope_text() {
        let envelope = StoredEnvelopeText::parse(r#"{"ok":18446744073709551616}"#.to_owned())
            .expect("the text is strict JSON")
            .expect("an object is an envelope text");
        let read = result_read_of(Ok(RawResultRead::Found(RawResultRecord {
            task_id: task(),
            task_name: "add".to_owned(),
            status: TaskStatus::Completed,
            raw_result: Some(envelope),
        })));
        assert_eq!(
            read,
            ResultRead::Found {
                record: ResultRecord {
                    task_id: text_of_uuid(task()),
                    task_name: "add".to_owned(),
                    status: TaskStatusName::Completed,
                    envelope: EnvelopeFact::Stored {
                        text: r#"{"ok":18446744073709551616}"#.to_owned()
                    },
                }
            }
        );
    }

    #[test]
    fn a_wait_that_ended_first_gives_the_status_and_no_envelope() {
        let read = result_read_of(Ok(RawResultRead::Found(RawResultRecord {
            task_id: task(),
            task_name: "add".to_owned(),
            status: TaskStatus::Running,
            raw_result: None,
        })));
        assert!(matches!(
            read,
            ResultRead::Found { record } if record.envelope == EnvelopeFact::NotFinished
                && record.status == TaskStatusName::Running
        ));
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
        let read = result_read_of(Err(BrokerOperationError::InvalidJsonPayload {
            message: "digest mismatch".to_owned(),
            cause: Box::new(BrokerCheckCause::Check),
        }));
        let ResultRead::Failed { error } = read else {
            panic!("a failed read is a catalog value");
        };
        assert_eq!(error.class, "BrokerInvalidJsonPayload");
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

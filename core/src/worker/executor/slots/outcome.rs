//! The hand-off from an [`AttemptEnd`] to the existing outcome writers
//! (Python executor design, section 4.7). One end gives one statement, then
//! phase 2 when the statement owes it.
//!
//! | End | Statement |
//! |---|---|
//! | `NotDispatched` | `horsies_release_claims` with placement `SAME_CLASS`; the row goes back to the queue |
//! | `Reported(result)`, a value | a plain task: its row in the next batch of the finalize batcher; a workflow task: the workflow success |
//! | `Reported(result)`, a task error | the failed attempt, with the retry the policy admits for its code |
//! | `Reported(result)` that does not read as the envelope | the failed attempt, `WORKER_SERIALIZATION_ERROR`, no retry |
//! | `Reported(stopped before running)` | none; an expired workflow task replays phase 2; an unconfirmed start releases the claim (`horsies_release_claims` with placement `SAME_CLASS`); the child's stop before start already wrote every other stop, the orphan cancel included |
//! | `Reported(worker failure)` | the failed attempt, `BROKER_ERROR`, no retry |
//! | `TimedOut`, `KilledForLoop`, `Crashed` | the failed attempt with `TASK_TIMEOUT`, the loop code or `WORKER_CRASHED`, the retry the policy admits, at the placement of the end, under the claimed-row rule `RELEASE`: a row that is still CLAIMED goes back to the queue in the same statement |
//! | `EndUnknown` | none: stale recovery or the lease decides the row |
//!
//! A database error never decides between a retry and a failure: the write
//! is sent again, at most the retries of its [`WriteStage`]. After the last
//! retry of a `TASK_TIMEOUT` write, crash recovery writes `TASK_TIMEOUT` at
//! the same placement, with its own retries (Python
//! `_recover_timed_out_row`). A crash-recovery write sent again stores
//! [`CRASH_RECOVERY_RETRY_PREFIX`] and the message of its first send (Python
//! `_retry_finalize_future`). A plain success sent again joins a later
//! batch; a row whose batch committed before its reply was lost answers
//! `AlreadyApplied`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde_json::value::RawValue;
use uuid::Uuid;

use super::dispatch::{
    AttemptEnd, ClaimedTask, LoopKillCode, NotRunCause, Settlement, TerminalState, TimeoutCancel,
    WrittenEnd,
};
use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, RetryPlacement, RetryRequest,
};
use crate::broker::claims::{
    release_claims, ReleaseBatch, ReleaseClaims, ReleaseOutcome, ReleasePlacement,
};
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::config::payload::{enforce_payload_policy, PayloadKind, PayloadPolicy};
use crate::core::lifecycle::{OwnedClaim, TerminalizationOutcome};
use crate::core::task::error::{
    BuiltInTaskCode, OperationalErrorCode, OutcomeCode, TaskError, TaskErrorCode,
};
use crate::core::task::result::{read_task_result_text, TaskResult, TaskResultText};
use crate::db::Transience;
use crate::telemetry::worker::WorkerTelemetry;
use crate::worker::execution::{
    persist_failed_attempt, persist_ok_result, persist_undecodable_result,
    persist_unreported_attempt, phase_retry_delay, run_phase2, FinalizeError, FinalizeOutcome,
    FinalizeStage, Phase2Work, PhaseTwoForm, UnreportedAttemptWrite, PHASE1_MAX_RETRIES,
};
use crate::worker::executor::protocol::{
    FailureDetail, ProcessId, ResultEnvelopeText, RunEnd, StopReason, TaskTimeoutMs,
};
use crate::worker::finalize_batch::statement::WorkerPoolSuccessWrite;
use crate::worker::finalize_batch::{FinalizeBatcher, PlainSuccess, RowNotWritten};
use crate::worker::retry::{admit_retry, RetryAdmission};
use crate::workflow_engine::child_spec::ChildSpecSource;

/// The text of a worker failure with no detail (Python
/// `_WORKER_FAILURE_FALLBACK`).
const WORKER_FAILURE_FALLBACK: &str = "Worker failure";

/// Retries of a `TASK_TIMEOUT` write (Python `_FINALIZE_TIMEOUT_MAX_RETRIES`).
pub const TIMEOUT_WRITE_MAX_RETRIES: u32 = 3;
/// Retries of a crash-recovery write (Python `_FINALIZE_FUTURE_MAX_RETRIES`).
pub const CRASH_RECOVERY_MAX_RETRIES: u32 = 3;
/// The start of the message of a crash-recovery write that is sent again
/// after a database error; the message of the first send follows. The
/// text is the one of `_retry_finalize_future` in
/// `core/worker/finalize.py` of the `v0.7.0` package.
pub const CRASH_RECOVERY_RETRY_PREFIX: &str = "Retry after future-stage finalize error: ";
/// Sends of the release of a claim that no child took, the first included.
pub const RELEASE_SENDS: u32 = 3;

/// The stage of an outcome write. Each stage has its own retry bound
/// (Python `_max_retries_of`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStage {
    /// The write of an end the child reported (Python `PHASE1`).
    PhaseOne,
    /// The `TASK_TIMEOUT` write of an attempt that reached its deadline
    /// (Python `TIMEOUT`).
    TimeoutWrite,
    /// The write of a child the worker killed or lost, and the last step of
    /// a `TASK_TIMEOUT` write whose retries all failed (Python `FUTURE`).
    CrashRecovery,
}

impl WriteStage {
    /// How many times a failed write of this stage is sent again.
    pub fn max_retries(self) -> u32 {
        match self {
            Self::PhaseOne => PHASE1_MAX_RETRIES,
            Self::TimeoutWrite => TIMEOUT_WRITE_MAX_RETRIES,
            Self::CrashRecovery => CRASH_RECOVERY_MAX_RETRIES,
        }
    }

    /// The stage name of the log lines: the value of Python's
    /// `FinalizeStage` for this stage.
    pub(crate) fn log_name(self) -> &'static str {
        match self {
            Self::PhaseOne => "phase1_persist",
            Self::TimeoutWrite => "timeout_write",
            Self::CrashRecovery => "future",
        }
    }
}

/// What one write of phase 1 did.
enum PhaseOneOutcome {
    /// The write applied or was refused; what it owes follows.
    Finalize(FinalizeOutcome),
    /// The row was still CLAIMED by this claim generation: the statement put
    /// it back in the queue with no attempt row.
    ClaimReleased,
}

/// One write of phase 1, sent again on a retryable database error.
enum PhaseOneWrite {
    Success { value: Vec<u8> },
    Undecodable { diagnosis: String },
    Failure(FinishFailedAttempt),
}

impl PhaseOneWrite {
    /// The state that this write sets when it applies as a terminal write.
    /// A success value comes from a result envelope that read as JSON, so
    /// its success write never falls back to `WORKER_SERIALIZATION_ERROR`.
    fn terminal_state(&self) -> TerminalState {
        match self {
            Self::Success { .. } => TerminalState::Completed,
            Self::Undecodable { .. } | Self::Failure(_) => TerminalState::Failed,
        }
    }
}

/// How the result envelope that a child reported reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultRead {
    /// The task returned a value.
    Value,
    /// The task returned an error with this code.
    TaskError(TaskErrorCode),
    /// The envelope does not read: the attempt fails with
    /// `WORKER_SERIALIZATION_ERROR`, never retried.
    Unreadable,
}

/// A result that a child reported, read once: how it reads, and the write
/// that records it.
pub struct ReadResult {
    read: ResultRead,
    write: PhaseOneWrite,
}

impl ReadResult {
    /// How the result envelope reads.
    pub fn read(&self) -> &ResultRead {
        &self.read
    }
}

/// One attempt end whose result envelope, when the child reported one, is
/// read once ([`OutcomeWriter::read_end`]). Each variant is the end of
/// [`AttemptEnd`] of the same name; a reported run is split into its three
/// forms.
pub enum ReadEnd {
    NotDispatched(NotRunCause),
    Result(ReadResult),
    StoppedBeforeRunning(StopReason),
    WorkerFailure(FailureDetail),
    TimedOut {
        timeout: TaskTimeoutMs,
        retry: RetryPlacement,
        cancel: TimeoutCancel,
    },
    KilledForLoop {
        code: LoopKillCode,
        retry: RetryPlacement,
        reason: FailureDetail,
    },
    Crashed {
        reason: FailureDetail,
    },
    EndUnknown {
        pid: ProcessId,
    },
}

/// What the lifecycle points read of an attempt end: the end, with a
/// reported result as it reads. Holds no payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndView<'a> {
    /// The task never reached a child.
    NotDispatched(NotRunCause),
    /// The child reported a result envelope.
    Result(&'a ResultRead),
    /// The child stopped before it set the row RUNNING.
    StoppedBeforeRunning(StopReason),
    /// The worker code of the child failed.
    WorkerFailure,
    /// The deadline decided.
    TimedOut,
    /// A blocked event loop ended the run.
    KilledForLoop(LoopKillCode),
    /// The child was lost with its exit seen.
    Crashed,
    /// The child was killed and its exit was not seen.
    EndUnknown { pid: ProcessId },
}

impl ReadEnd {
    /// What the lifecycle points read of this end.
    pub fn view(&self) -> EndView<'_> {
        match self {
            Self::NotDispatched(cause) => EndView::NotDispatched(*cause),
            Self::Result(result) => EndView::Result(&result.read),
            Self::StoppedBeforeRunning(reason) => EndView::StoppedBeforeRunning(*reason),
            Self::WorkerFailure(_) => EndView::WorkerFailure,
            Self::TimedOut { .. } => EndView::TimedOut,
            Self::KilledForLoop { code, .. } => EndView::KilledForLoop(*code),
            Self::Crashed { .. } => EndView::Crashed,
            Self::EndUnknown { pid } => EndView::EndUnknown { pid: *pid },
        }
    }
}

/// The write of attempt ends: the [`OutcomeWriter`] of a worker, or a
/// scripted writer in a test.
pub trait EndWrites: Send + Sync {
    /// An end whose result envelope is read.
    type Read: Send;

    /// Read `end` once: the result envelope of a reported result is parsed
    /// here and nowhere else. Writes nothing.
    fn read_end(&self, task: &ClaimedTask, end: AttemptEnd) -> Self::Read;

    /// What the lifecycle points read of `read`.
    fn view(read: &Self::Read) -> EndView<'_>;

    /// Write the end that `read` holds, then phase 2 when it is owed.
    fn write_end(
        &self,
        task: &ClaimedTask,
        read: Self::Read,
    ) -> impl Future<Output = Settlement> + Send;
}

/// What the failed-attempt statement does with a row that is still CLAIMED
/// by this claim generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimedRowRule {
    /// The child reported a result, so the row must be RUNNING; a CLAIMED
    /// row is not held (`horsies_finish_failed_attempt` with `REFUSE`).
    Refuse,
    /// A killed child, which may have died before it set the row RUNNING: a
    /// CLAIMED row goes back to the queue with no attempt, in the same
    /// statement (`RELEASE`).
    Release,
}

/// The retry that the task's policy admits for `code` (Python
/// `retry_request_for`). Pure.
pub fn retry_request(
    task: &ClaimedTask,
    code: Option<&str>,
    placement: RetryPlacement,
) -> RetryRequest {
    match admit_retry(
        task.retry.retry_count,
        task.retry.max_retries,
        task.options.retry(),
        code,
    ) {
        RetryAdmission::Admitted(rule) => RetryRequest::RetryAfter {
            expected_retry_count: task.retry.retry_count,
            delay_seconds: rule.delay_seconds((task.retry.retry_count + 1).unsigned_abs()),
            placement,
        },
        RetryAdmission::RetriesSpent | RetryAdmission::NoPolicy | RetryAdmission::CodeNotListed => {
            RetryRequest::NoRetry
        }
    }
}

fn fence(task: &ClaimedTask, worker_id: &str) -> OwnedClaim {
    OwnedClaim {
        worker_id: worker_id.to_owned(),
        claimed_at: task.claimed_at,
    }
}

/// The error envelope of a failure the worker writes.
fn error_envelope(error: TaskError) -> String {
    serde_json::to_string(&TaskResult::<serde_json::Value>::Err(error))
        .expect("a task error holds only JSON values, so its envelope encodes")
}

/// The failed attempt of a task error the child reported. Pure.
pub fn task_error_write(
    task: &ClaimedTask,
    worker_id: &str,
    error: &TaskError,
    envelope: &ResultEnvelopeText,
) -> FinishFailedAttempt {
    let error_code = Some(error.error_code.to_string());
    FinishFailedAttempt {
        task_id: task.task_id,
        fence: fence(task, worker_id),
        retry: retry_request(task, error_code.as_deref(), RetryPlacement::SameClass),
        result_json: envelope.as_str().to_owned(),
        error_code,
        failed_reason: None,
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: error.message.clone(),
            failed_reason: None,
        },
    }
}

/// The failed attempt of a failure of the child's worker code (Python
/// `_worker_failure_command`): `BROKER_ERROR`, never retried. Pure.
pub fn worker_failure_write(
    task: &ClaimedTask,
    worker_id: &str,
    detail: &str,
) -> FinishFailedAttempt {
    let message = match detail.is_empty() {
        true => WORKER_FAILURE_FALLBACK.to_owned(),
        false => detail.to_owned(),
    };
    let mut error = TaskError::builtin(OperationalErrorCode::BrokerError, message.clone());
    error.data = Some(crate::core::codec::JsonText::of_engine_value(
        &serde_json::json!({ "task_id": task.task_id.to_string() }),
    ));
    FinishFailedAttempt {
        task_id: task.task_id,
        fence: fence(task, worker_id),
        retry: RetryRequest::NoRetry,
        result_json: error_envelope(error),
        error_code: Some(OperationalErrorCode::BrokerError.to_string()),
        failed_reason: Some(message.clone()),
        attempt: AttemptFailure {
            kind: AttemptFailureKind::WorkerFailure,
            error_message: Some(message.clone()),
            failed_reason: Some(message),
        },
    }
}

/// The failed attempt of a child the worker killed or lost: the code of
/// the end, the retry the policy admits for it, at `placement`, with
/// Python's error `data`: `task_id` and `worker_id` (Python
/// `_recover_worker_future_failure`). The attempt records `message` as its
/// message and its failed reason; the task's failed reason is not written
/// (Python `_write_ended_attempt`). Pure.
pub fn killed_child_write(
    task: &ClaimedTask,
    worker_id: &str,
    code: BuiltInTaskCode,
    message: String,
    placement: RetryPlacement,
) -> FinishFailedAttempt {
    let code_text = code.value();
    let mut error = TaskError::builtin(code, message.clone());
    error.data = Some(crate::core::codec::JsonText::of_engine_value(
        &serde_json::json!({
            "task_id": task.task_id.to_string(),
            "worker_id": worker_id,
        }),
    ));
    FinishFailedAttempt {
        task_id: task.task_id,
        fence: fence(task, worker_id),
        retry: retry_request(task, Some(&code_text), placement),
        result_json: error_envelope(error),
        error_code: Some(code_text),
        failed_reason: None,
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some(message.clone()),
            failed_reason: Some(message),
        },
    }
}

/// The two forms of one crash-recovery write (Python stage `FUTURE`).
#[derive(Debug, Clone, PartialEq)]
pub struct CrashRecoveryWrites {
    /// The first send.
    pub first: FinishFailedAttempt,
    /// Each send after a database error that may clear: the same code,
    /// retry, fence and error `data`, with [`CRASH_RECOVERY_RETRY_PREFIX`]
    /// before the message of `first`.
    pub retry: FinishFailedAttempt,
}

/// The crash-recovery writes of a child the worker killed or lost: two
/// [`killed_child_write`]s, with `message` on the first send and with
/// `Retry after future-stage finalize error: <message>` on each send after
/// a database error. Pure.
pub fn crash_recovery_writes(
    task: &ClaimedTask,
    worker_id: &str,
    code: BuiltInTaskCode,
    message: String,
    placement: RetryPlacement,
) -> CrashRecoveryWrites {
    let retry_message = format!("{CRASH_RECOVERY_RETRY_PREFIX}{message}");
    CrashRecoveryWrites {
        first: killed_child_write(task, worker_id, code.clone(), message, placement),
        retry: killed_child_write(task, worker_id, code, retry_message, placement),
    }
}

/// The `TASK_TIMEOUT` write of an attempt that reached `timeout`, with
/// Python's error `data`: `task_id`, `timeout_ms`, and, for a shared async
/// run, `cancel_end` (Python `_settle_timed_out_row`). The attempt records
/// the timeout text as its message and its failed reason; the task's
/// failed reason is not written (Python `_write_ended_attempt`). Pure.
pub fn timeout_write(
    task: &ClaimedTask,
    worker_id: &str,
    timeout: TaskTimeoutMs,
    placement: RetryPlacement,
    cancel: TimeoutCancel,
) -> FinishFailedAttempt {
    let code: BuiltInTaskCode = OutcomeCode::TaskTimeout.into();
    let code_text = code.value();
    let message = format!("Task exceeded timeout_ms={}", timeout.get());
    let mut error = TaskError::builtin(code, message.clone());
    error.data = Some(crate::core::codec::JsonText::of_engine_value(
        &timeout_data(task.task_id, timeout, cancel),
    ));
    FinishFailedAttempt {
        task_id: task.task_id,
        fence: fence(task, worker_id),
        retry: retry_request(task, Some(&code_text), placement),
        result_json: error_envelope(error),
        error_code: Some(code_text),
        failed_reason: None,
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some(message.clone()),
            failed_reason: Some(message),
        },
    }
}

/// The `data` of a `TASK_TIMEOUT` error. `cancel_end` is set for a shared
/// async run and for no other run. Pure.
fn timeout_data(task_id: Uuid, timeout: TaskTimeoutMs, cancel: TimeoutCancel) -> serde_json::Value {
    let mut data = serde_json::json!({
        "task_id": task_id.to_string(),
        "timeout_ms": timeout.get(),
    });
    match cancel {
        TimeoutCancel::Ended(end) => {
            data["cancel_end"] = serde_json::Value::from(end.as_str());
        }
        TimeoutCancel::NoSignal => {}
    }
    data
}

/// The crash-recovery writes of an attempt that reached `timeout` and whose
/// `TASK_TIMEOUT` write failed on every retry: the same code and placement,
/// with a message that names the failed writes, and with the `data` of
/// crash recovery (Python `_recover_timed_out_row`). Pure.
pub fn timeout_recovery_writes(
    task: &ClaimedTask,
    worker_id: &str,
    timeout: TaskTimeoutMs,
    placement: RetryPlacement,
) -> CrashRecoveryWrites {
    crash_recovery_writes(
        task,
        worker_id,
        OutcomeCode::TaskTimeout.into(),
        format!(
            "Task reached timeout_ms={}, and the TASK_TIMEOUT write failed {} times",
            timeout.get(),
            TIMEOUT_WRITE_MAX_RETRIES + 1,
        ),
        placement,
    )
}

/// The stored form of a success value (`TaskResult::Ok`), with the value
/// text kept byte for byte. Pure.
///
/// Errors: the diagnosis of a value that is not JSON; the caller fails the
/// task with `WORKER_SERIALIZATION_ERROR`.
fn ok_envelope(value: &[u8]) -> Result<String, String> {
    let raw = serde_json::from_slice::<&RawValue>(value).map_err(|error| error.to_string())?;
    serde_json::to_string(&TaskResult::Ok(raw)).map_err(|error| error.to_string())
}

/// A finalize batcher for an outcome writer under test: a worker pool of 5
/// connections (2 flushers) and a queue of 64 rows.
#[cfg(test)]
pub(crate) fn test_successes(
    broker: &Arc<PostgresBroker>,
    worker_id: &str,
) -> FinalizeBatcher<WorkerPoolSuccessWrite> {
    use crate::db::{PoolSizing, Reserve, WORKER_RESERVED_CONNECTIONS};
    use crate::worker::finalize_batch::FlusherBound;
    let sizing = PoolSizing::new(5, Reserve::KeepFree(WORKER_RESERVED_CONNECTIONS))
        .expect("a worker pool of 5 keeps batch connections");
    FinalizeBatcher::new(
        WorkerPoolSuccessWrite::new(Arc::clone(broker), worker_id.to_owned()),
        std::num::NonZeroUsize::new(64).expect("64 is not zero"),
        FlusherBound::of_worker_pool(&sizing).expect("a pool of 5 leaves 2 flushers"),
    )
}

/// The settlement of one release, from its one outcome. The release answers
/// each input row once, so another count is a defect.
fn released(task: &ClaimedTask, reason: &str, outcomes: &[(Uuid, ReleaseOutcome)]) -> Settlement {
    match outcomes {
        [(_, ReleaseOutcome::Released)] => {
            tracing::warn!(
                task_id = %task.task_id,
                "Requeued worker-owned in-flight task {}: {reason}",
                task.task_id,
            );
            Settlement::Written(WrittenEnd::ClaimReleased)
        }
        [(_, ReleaseOutcome::NotHeld)] => {
            tracing::warn!(
                task_id = %task.task_id,
                "Failed to requeue task {} (not owned or not requeueable): {reason}",
                task.task_id,
            );
            Settlement::NotHeld
        }
        other => unreachable!(
            "the release of one claim answered {} rows for task {}",
            other.len(),
            task.task_id
        ),
    }
}

/// The settlement of a release that failed: the claim lease returns the
/// row.
fn release_failed(
    task: &ClaimedTask,
    reason: &str,
    error: &BrokerError,
    retryable: bool,
) -> Settlement {
    tracing::error!(
        task_id = %task.task_id,
        error = %error,
        "DB error while requeueing task {} ({reason}): {error}",
        task.task_id,
    );
    match retryable {
        true => Settlement::RetriesExhausted,
        false => Settlement::WriteFailed,
    }
}

/// Read `end` once: the result envelope of a reported result is parsed
/// here and nowhere else, into the write that records it (a task error
/// write names `worker_id` in its fence). The size of the envelope gets the
/// warning of `payload_policy`. Writes nothing.
pub fn read_attempt_end(
    task: &ClaimedTask,
    end: AttemptEnd,
    worker_id: &str,
    payload_policy: &PayloadPolicy,
) -> ReadEnd {
    match end {
        AttemptEnd::NotDispatched(cause) => ReadEnd::NotDispatched(cause),
        AttemptEnd::Reported(RunEnd::Result(envelope)) => {
            ReadEnd::Result(read_result(task, &envelope, worker_id, payload_policy))
        }
        AttemptEnd::Reported(RunEnd::StoppedBeforeRunning(reason)) => {
            ReadEnd::StoppedBeforeRunning(reason)
        }
        AttemptEnd::Reported(RunEnd::WorkerFailure(detail)) => ReadEnd::WorkerFailure(detail),
        AttemptEnd::TimedOut {
            timeout,
            retry,
            cancel,
        } => ReadEnd::TimedOut {
            timeout,
            retry,
            cancel,
        },
        AttemptEnd::KilledForLoop {
            code,
            retry,
            reason,
        } => ReadEnd::KilledForLoop {
            code,
            retry,
            reason,
        },
        AttemptEnd::Crashed { reason } => ReadEnd::Crashed { reason },
        AttemptEnd::EndUnknown { pid } => ReadEnd::EndUnknown { pid },
    }
}

/// The read of a result envelope that a child reported, and its write.
fn read_result(
    task: &ClaimedTask,
    envelope: &ResultEnvelopeText,
    worker_id: &str,
    payload_policy: &PayloadPolicy,
) -> ReadResult {
    // Warn-only: a result is never refused over size; the work is done.
    enforce_payload_policy(
        payload_policy,
        &task.task_name,
        PayloadKind::Result,
        envelope.as_str().len(),
    );
    match read_task_result_text(envelope.as_str()) {
        Ok(TaskResultText::Ok(value)) => ReadResult {
            read: ResultRead::Value,
            write: PhaseOneWrite::Success {
                value: value.get().as_bytes().to_vec(),
            },
        },
        Ok(TaskResultText::Err(error)) => ReadResult {
            read: ResultRead::TaskError(error.error_code.clone()),
            write: PhaseOneWrite::Failure(task_error_write(task, worker_id, &error, envelope)),
        },
        Err(refused) => ReadResult {
            read: ResultRead::Unreadable,
            write: PhaseOneWrite::Undecodable {
                diagnosis: refused.to_string(),
            },
        },
    }
}

/// Writes the end of each attempt with the existing statements.
///
/// The plain successes go to the finalize batcher of the worker, which
/// writes them in batches; the worker stop closes it after the drain of the
/// outcome writes (`finalize_batch::stop::drain_then_close`).
pub struct OutcomeWriter {
    broker: Arc<PostgresBroker>,
    /// Where phase 2 gets the spec of a sub-workflow child that it starts.
    child_specs: Arc<dyn ChildSpecSource>,
    worker_id: String,
    payload_policy: PayloadPolicy,
    successes: FinalizeBatcher<WorkerPoolSuccessWrite>,
    /// Where each finalize write sent again is counted.
    telemetry: WorkerTelemetry,
}

impl OutcomeWriter {
    pub fn new(
        broker: Arc<PostgresBroker>,
        child_specs: Arc<dyn ChildSpecSource>,
        worker_id: String,
        payload_policy: PayloadPolicy,
        successes: FinalizeBatcher<WorkerPoolSuccessWrite>,
    ) -> Self {
        Self {
            broker,
            child_specs,
            worker_id,
            payload_policy,
            successes,
            telemetry: WorkerTelemetry::Off,
        }
    }

    /// This writer, counting each finalize write sent again in `telemetry`
    /// (`horsies.finalize.retries{stage}`).
    pub fn with_telemetry(self, telemetry: WorkerTelemetry) -> Self {
        Self { telemetry, ..self }
    }

    /// Write the end of one attempt, then phase 2 when it is owed: the read
    /// of [`OutcomeWriter::read_end`], then [`OutcomeWriter::write_end`].
    pub async fn settle(&self, task: &ClaimedTask, end: AttemptEnd) -> Settlement {
        let read = self.read_end(task, end);
        self.write_end(task, read).await
    }

    /// Read `end` once ([`read_attempt_end`] with the worker id and the
    /// payload policy of this writer). Writes nothing.
    pub fn read_end(&self, task: &ClaimedTask, end: AttemptEnd) -> ReadEnd {
        read_attempt_end(task, end, &self.worker_id, &self.payload_policy)
    }

    /// Write the end that `end` holds, then phase 2 when it is owed.
    pub async fn write_end(&self, task: &ClaimedTask, end: ReadEnd) -> Settlement {
        match end {
            ReadEnd::NotDispatched(cause) => self.release(task, &cause.to_string()).await,
            ReadEnd::EndUnknown { pid } => {
                tracing::warn!(
                    task_id = %task.task_id,
                    pid = pid.get(),
                    "the exit of the task's child was not seen; recovery decides the row",
                );
                Settlement::LeftToRecovery
            }
            ReadEnd::StoppedBeforeRunning(reason) => self.after_stop(task, reason).await,
            ReadEnd::Result(result) => {
                self.phase_one(
                    task,
                    result.write,
                    ClaimedRowRule::Refuse,
                    WriteStage::PhaseOne,
                )
                .await
            }
            ReadEnd::WorkerFailure(detail) => {
                let write = worker_failure_write(task, &self.worker_id, detail.as_str());
                self.phase_one(
                    task,
                    PhaseOneWrite::Failure(write),
                    ClaimedRowRule::Refuse,
                    WriteStage::PhaseOne,
                )
                .await
            }
            ReadEnd::TimedOut {
                timeout,
                retry,
                cancel,
            } => self.timed_out(task, timeout, retry, cancel).await,
            ReadEnd::KilledForLoop {
                code,
                retry,
                reason,
            } => {
                let writes = crash_recovery_writes(
                    task,
                    &self.worker_id,
                    code.code().into(),
                    reason.into_string(),
                    retry,
                );
                self.crash_recovery(task, writes).await
            }
            ReadEnd::Crashed { reason } => {
                let writes = crash_recovery_writes(
                    task,
                    &self.worker_id,
                    OperationalErrorCode::WorkerCrashed.into(),
                    reason.into_string(),
                    RetryPlacement::SameClass,
                );
                self.crash_recovery(task, writes).await
            }
        }
    }

    /// The `TASK_TIMEOUT` write and its retries. When every retry failed,
    /// crash recovery writes `TASK_TIMEOUT` at the same placement, with its
    /// own retries (Python `_after_the_last_retry`,
    /// `_recover_timed_out_row`). A write that no retry can clear ends at
    /// once: crash recovery would send the same statement.
    async fn timed_out(
        &self,
        task: &ClaimedTask,
        timeout: TaskTimeoutMs,
        placement: RetryPlacement,
        cancel: TimeoutCancel,
    ) -> Settlement {
        let write = timeout_write(task, &self.worker_id, timeout, placement, cancel);
        let settled = self
            .phase_one(
                task,
                PhaseOneWrite::Failure(write),
                ClaimedRowRule::Release,
                WriteStage::TimeoutWrite,
            )
            .await;
        match settled {
            Settlement::RetriesExhausted => {}
            Settlement::Written(_)
            | Settlement::NothingOwed
            | Settlement::NotHeld
            | Settlement::LeftToRecovery
            | Settlement::WriteFailed => return settled,
        }
        tracing::error!(
            task_id = %task.task_id,
            "every retry of the TASK_TIMEOUT write failed; crash recovery writes the row",
        );
        let recovery = timeout_recovery_writes(task, &self.worker_id, timeout, placement);
        self.crash_recovery(task, recovery).await
    }

    async fn write_once(
        &self,
        task: &ClaimedTask,
        write: &PhaseOneWrite,
        claimed_row: ClaimedRowRule,
    ) -> Result<PhaseOneOutcome, FinalizeError> {
        let workflow = task.kind.is_workflow_task();
        let written = match (write, claimed_row) {
            (PhaseOneWrite::Success { value }, _) => match task.kind.is_workflow_task() {
                true => {
                    persist_ok_result(
                        &self.broker,
                        task.task_id,
                        value,
                        fence(task, &self.worker_id),
                        workflow,
                    )
                    .await
                }
                false => self.complete_plain_success(task, value).await,
            },
            (PhaseOneWrite::Undecodable { diagnosis }, _) => {
                persist_undecodable_result(
                    &self.broker,
                    task.task_id,
                    fence(task, &self.worker_id),
                    workflow,
                    diagnosis,
                )
                .await
            }
            (PhaseOneWrite::Failure(command), ClaimedRowRule::Refuse) => {
                persist_failed_attempt(&self.broker, command, workflow).await
            }
            (PhaseOneWrite::Failure(command), ClaimedRowRule::Release) => {
                return persist_unreported_attempt(&self.broker, command, workflow)
                    .await
                    .map(|written| match written {
                        UnreportedAttemptWrite::ClaimReleased => PhaseOneOutcome::ClaimReleased,
                        UnreportedAttemptWrite::Written(outcome) => {
                            PhaseOneOutcome::Finalize(outcome)
                        }
                    });
            }
        };
        written.map(PhaseOneOutcome::Finalize)
    }

    /// The success of a plain task: its row joins the next batch of the
    /// finalize batcher. Returns after the
    /// statement of the row's batch returned. A plain success owes no phase
    /// 2: its capacity wake commits with the row.
    ///
    /// Errors: a [`FinalizeError`] of phase 1, retryable when the cause of
    /// the batch write may clear (an unknown outcome is retryable: the
    /// replay finds the row `AlreadyApplied` when it committed).
    async fn complete_plain_success(
        &self,
        task: &ClaimedTask,
        value: &[u8],
    ) -> Result<FinalizeOutcome, FinalizeError> {
        let result_json = match ok_envelope(value) {
            Ok(result_json) => result_json,
            Err(diagnosis) => {
                return persist_undecodable_result(
                    &self.broker,
                    task.task_id,
                    fence(task, &self.worker_id),
                    false,
                    &diagnosis,
                )
                .await
            }
        };
        let answer = self
            .successes
            .complete(PlainSuccess {
                task_id: task.task_id,
                claimed_at: task.claimed_at,
                result_json,
            })
            .await;
        match answer {
            Ok(
                TerminalizationOutcome::Applied { .. }
                | TerminalizationOutcome::AlreadyApplied { .. },
            ) => Ok(FinalizeOutcome::Finalized),
            Ok(
                TerminalizationOutcome::LostClaim { .. }
                | TerminalizationOutcome::SourceStateConflict { .. }
                | TerminalizationOutcome::TaskAbsent { .. },
            ) => Ok(FinalizeOutcome::Refused),
            Err(RowNotWritten::WriteFailed(error)) => Err(FinalizeError {
                stage: FinalizeStage::Phase1Persist,
                task_id: task.task_id,
                message: format!("the batched success write failed: {error}"),
                retryable: matches!(error.transience(), Transience::Transient),
            }),
            Err(
                stopped @ (RowNotWritten::StatementCancelledAtStop | RowNotWritten::NotSentAtStop),
            ) => Err(FinalizeError {
                stage: FinalizeStage::Phase1Persist,
                task_id: task.task_id,
                message: stopped.to_string(),
                retryable: false,
            }),
        }
    }

    /// The write and at most the retries of its `stage`, each retry the same
    /// write (Python `_retry_finalize_phase1`, `_retry_finalize_timeout`);
    /// then phase 2 when owed.
    async fn phase_one(
        &self,
        task: &ClaimedTask,
        write: PhaseOneWrite,
        claimed_row: ClaimedRowRule,
        stage: WriteStage,
    ) -> Settlement {
        self.send_with_retries(task, &write, &write, claimed_row, stage)
            .await
    }

    /// A crash-recovery write under the claimed-row rule `RELEASE`:
    /// `writes.first`, then `writes.retry` on each retry of
    /// [`WriteStage::CrashRecovery`] (Python `_retry_finalize_future`); then
    /// phase 2 when owed.
    async fn crash_recovery(&self, task: &ClaimedTask, writes: CrashRecoveryWrites) -> Settlement {
        let first = PhaseOneWrite::Failure(writes.first);
        let retry = PhaseOneWrite::Failure(writes.retry);
        self.send_with_retries(
            task,
            &first,
            &retry,
            ClaimedRowRule::Release,
            WriteStage::CrashRecovery,
        )
        .await
    }

    /// Send `first`, then `retry` after each database error that may clear,
    /// at most the retries of `stage`; then phase 2 when owed.
    ///
    /// The write is boxed: its future holds every writer of phase 1, and
    /// `settle` builds this future at seven call sites. Inline, a debug
    /// build needs more than a 2 MiB thread stack for one settlement.
    async fn send_with_retries(
        &self,
        task: &ClaimedTask,
        first: &PhaseOneWrite,
        retry: &PhaseOneWrite,
        claimed_row: ClaimedRowRule,
        stage: WriteStage,
    ) -> Settlement {
        let max_retries = stage.max_retries();
        let mut retries_done: u32 = 0;
        let outcome = loop {
            let write = match retries_done {
                0 => first,
                _ => retry,
            };
            let error = match Box::pin(self.write_once(task, write, claimed_row)).await {
                Ok(outcome) => break outcome,
                Err(error) => error,
            };
            match (error.retryable, retries_done < max_retries) {
                (true, true) => {
                    retries_done += 1;
                    self.telemetry.finalize_retried(stage);
                    let delay = phase_retry_delay(retries_done);
                    tracing::warn!(
                        task_id = %task.task_id,
                        stage = ?stage,
                        retry = retries_done,
                        max = max_retries,
                        error = %error,
                        "Finalize retry scheduled for task {} stage={} \
                         ({retries_done}/{max_retries}) in {delay:.1}s: {error}",
                        task.task_id,
                        stage.log_name(),
                    );
                    tokio::time::sleep(Duration::from_secs_f64(delay)).await;
                }
                (true, false) => {
                    tracing::error!(
                        task_id = %task.task_id,
                        stage = ?stage,
                        retries = max_retries,
                        error = %error,
                        "Finalize retries exhausted for task {} stage={} after {max_retries} \
                         attempts: {error}",
                        task.task_id,
                        stage.log_name(),
                    );
                    return Settlement::RetriesExhausted;
                }
                (false, _) => {
                    tracing::error!(
                        task_id = %task.task_id,
                        stage = ?stage,
                        error = %error,
                        "Finalize stage will not be re-attempted ({}) stage={}: {error}",
                        task.task_id,
                        stage.log_name(),
                    );
                    return Settlement::WriteFailed;
                }
            }
        };
        // `first` and `retry` are writes of one kind: both set one state.
        let terminal = first.terminal_state();
        match outcome {
            PhaseOneOutcome::Finalize(FinalizeOutcome::Terminal {
                capacity_notified,
                phase_two,
            }) => {
                self.phase_two(task, capacity_notified, phase_two).await;
                Settlement::Written(WrittenEnd::Terminal(terminal))
            }
            PhaseOneOutcome::Finalize(FinalizeOutcome::Finalized) => {
                Settlement::Written(WrittenEnd::Terminal(terminal))
            }
            PhaseOneOutcome::Finalize(FinalizeOutcome::Retried {
                retry_count,
                next_retry_at,
            }) => Settlement::Written(WrittenEnd::RetryScheduled {
                retry_count,
                next_retry_at,
            }),
            PhaseOneOutcome::Finalize(FinalizeOutcome::Refused) => Settlement::NotHeld,
            PhaseOneOutcome::ClaimReleased => {
                tracing::warn!(
                    task_id = %task.task_id,
                    "the child was killed before the task ran; the claim went back to the queue",
                );
                Settlement::Written(WrittenEnd::ClaimReleased)
            }
        }
    }

    /// Phase 2 of one task. Boxed: the future of `run_phase2` holds the node
    /// finish and the parent advances, and `phase_one` and `after_stop`
    /// would each carry it inline.
    async fn phase_two(
        &self,
        task: &ClaimedTask,
        capacity_notified: bool,
        phase_two: PhaseTwoForm,
    ) {
        Box::pin(run_phase2(
            &self.broker,
            self.child_specs.as_ref(),
            Phase2Work {
                task_id: task.task_id,
                queue_name: task.queue_name.clone(),
                is_workflow_task: task.kind.is_workflow_task(),
                capacity_notified,
                phase_two,
            },
        ))
        .await;
    }

    /// Release a claim that no child took: `horsies_release_claims`
    /// with placement `SAME_CLASS`, fenced to this worker and this claim
    /// generation. The row goes back to the queue in its class; a row in any
    /// other state is left alone. A database error that may clear sends the
    /// release again, at most [`RELEASE_SENDS`] sends in all. After an
    /// unknown outcome a released row answers `NotHeld` to the next send.
    async fn release(&self, task: &ClaimedTask, reason: &str) -> Settlement {
        let batch = match ReleaseBatch::new(&[(task.task_id, task.claimed_at)]) {
            Ok(batch) => batch,
            Err(refused) => unreachable!("one claim is a valid release batch: {refused}"),
        };
        let command = ReleaseClaims {
            worker_id: self.worker_id.clone(),
            batch,
            placement: ReleasePlacement::SameClass,
        };
        let mut sends: u32 = 1;
        loop {
            let sent = match self.broker.core_pools_for_write() {
                Ok((open, _write)) => {
                    release_claims(open.main(), self.broker.statement_log(), &command).await
                }
                Err(error) => Err(error),
            };
            let error = match sent {
                Ok(outcomes) => return released(task, reason, &outcomes),
                Err(error) => error,
            };
            match (error.is_retryable(), sends < RELEASE_SENDS) {
                (true, true) => {
                    tracing::warn!(
                        task_id = %task.task_id,
                        send = sends,
                        max = RELEASE_SENDS,
                        error = %error,
                        "the release of the claim failed: {error}; sending it again",
                    );
                    tokio::time::sleep(Duration::from_secs_f64(phase_retry_delay(sends))).await;
                    sends += 1;
                }
                (retryable, _) => return release_failed(task, reason, &error, retryable),
            }
        }
    }

    /// The child stopped before RUNNING; no task code ran.
    ///
    /// - An expired workflow task replays phase 2.
    /// - An unconfirmed start releases the claim at once with the fenced
    ///   release (Python `ReleaseUnconfirmedClaim`): the row goes back to the
    ///   queue with its class only while it is CLAIMED by this worker at this
    ///   claim generation. A row outside the fence, such as a start that
    ///   committed before its reply was lost, is left to recovery. A release
    ///   that fails is logged, and the claim lease returns the row.
    /// - A task whose node is not linked was not cancelled by the child's
    ///   stop before start, because the app turns the self-heal off: the row
    ///   stays CLAIMED for inspection.
    /// - Every other stop wrote what its case needs in the child; an
    ///   orphan that the child's stop function cancelled owes nothing more.
    async fn after_stop(&self, task: &ClaimedTask, reason: StopReason) -> Settlement {
        match (reason, task.kind.is_workflow_task()) {
            (StopReason::TaskExpired, true) => {
                self.phase_two(task, false, PhaseTwoForm::NotKnown).await;
                Settlement::NothingOwed
            }
            (StopReason::OwnershipUnconfirmed, _) => {
                self.release(task, "the start of the attempt was not confirmed")
                    .await
            }
            (StopReason::NodeNotLinked, _) => {
                tracing::debug!(
                    task_id = %task.task_id,
                    "Task {} terminalization classified as no-op: ORPHAN_SELF_HEAL_DISABLED",
                    task.task_id,
                );
                Settlement::NothingOwed
            }
            (
                StopReason::TaskExpired
                | StopReason::ClaimLost
                | StopReason::WorkflowStopped
                | StopReason::OrphanCancelled,
                _,
            ) => Settlement::NothingOwed,
        }
    }
}

impl EndWrites for OutcomeWriter {
    type Read = ReadEnd;

    fn read_end(&self, task: &ClaimedTask, end: AttemptEnd) -> ReadEnd {
        OutcomeWriter::read_end(self, task, end)
    }

    fn view(read: &ReadEnd) -> EndView<'_> {
        read.view()
    }

    fn write_end(
        &self,
        task: &ClaimedTask,
        read: ReadEnd,
    ) -> impl Future<Output = Settlement> + Send {
        OutcomeWriter::write_end(self, task, read)
    }
}

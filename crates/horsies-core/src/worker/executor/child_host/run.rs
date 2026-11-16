//! One run in a Rust child: the start write, the refusal or the body, the
//! result envelope, the finalizing marker. The order is Python's (executor
//! design, section 4.8): start, user code, encode, marker, frame.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::value::RawValue;
use uuid::Uuid;

use super::host::{ChildApp, OrphanSelfHeal};
use super::link::FrameWriter;
use crate::broker::PostgresBroker;
use crate::core::task::error::{OperationalErrorCode, TaskError};
use crate::core::task::fn_trait::RegisteredTask;
use crate::core::task::result::TaskResult;
use crate::worker::execution::{
    build_task_input, confirm_ownership_and_set_running, mark_task_finalizing, FinalizingMark,
    OwnershipOutcome,
};
use crate::worker::executor::protocol::{
    FailureDetail, ResultEnvelopeText, RunEnd, StopReason, StoredOptionsRefusal, TaskInputText,
    TaskKind, MAX_BODY_BYTES,
};

/// What every run of one child shares.
#[derive(Clone)]
pub(super) struct HostContext {
    pub(super) broker: Arc<PostgresBroker>,
    pub(super) app: Arc<ChildApp>,
    pub(super) pid: i32,
    pub(super) writer: FrameWriter,
}

/// The facts of one run that every write fences on.
#[derive(Debug, Clone)]
pub(super) struct RunFacts {
    pub(super) task_id: Uuid,
    pub(super) task_name: String,
    pub(super) worker_id: String,
    pub(super) task_kind: TaskKind,
    pub(super) claimed_at: DateTime<Utc>,
}

/// How the start write ended.
pub(super) enum Started {
    Running,
    Stopped(StopReason),
}

/// The start write: CLAIMED to RUNNING under the fence, with the first
/// runner beat and, for a workflow task, the node. A start that misses ran
/// its miss path already; the run ends before any task code.
///
/// The core's start answers one "not started" for a lost claim and a
/// stopped workflow; both owe nothing more, and they are reported as a lost
/// claim until the start statement names each reason.
pub(super) async fn start_attempt(context: &HostContext, facts: &RunFacts) -> Started {
    let outcome = confirm_ownership_and_set_running(
        &context.broker,
        facts.task_id,
        &facts.worker_id,
        context.pid,
        &context.app.hostname,
        facts.task_kind.is_workflow_task(),
        facts.claimed_at,
        matches!(context.app.orphan_self_heal, OrphanSelfHeal::On),
    )
    .await;
    match outcome {
        OwnershipOutcome::Running(_) => Started::Running,
        OwnershipOutcome::ExpiredBeforeStart => Started::Stopped(StopReason::TaskExpired),
        OwnershipOutcome::Aborted => Started::Stopped(StopReason::ClaimLost),
    }
}

/// The finalizing marker of a run that set its row RUNNING. A failed write
/// is logged: the parent's write is fenced on its own.
pub(super) async fn mark_finalizing(context: &HostContext, facts: &RunFacts) {
    match mark_task_finalizing(
        context.broker.pool(),
        facts.task_id,
        &facts.worker_id,
        facts.claimed_at,
    )
    .await
    {
        Ok(FinalizingMark::Marked) => {}
        Ok(FinalizingMark::NotHeld) => {
            tracing::debug!(task_id = %facts.task_id, "finalizing marker skipped: the claim changed");
        }
        Err(error) => {
            tracing::warn!(task_id = %facts.task_id, error = %error, "the finalizing marker write failed");
        }
    }
}

/// The envelope of a task error.
fn error_envelope(error: TaskError) -> RunEnd {
    let text = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(error))
        .expect("a task error holds only JSON values, so its envelope encodes");
    RunEnd::Result(ResultEnvelopeText::new(text))
}

/// The end of a run whose stored options the worker cannot use: no task
/// code runs, and the task fails with `WORKER_SERIALIZATION_ERROR`.
pub(super) fn refusal_end(refusal: &StoredOptionsRefusal) -> RunEnd {
    let reason = match refusal {
        StoredOptionsRefusal::OptionsUnreadable(reason)
        | StoredOptionsRefusal::TimeoutValueInvalid(reason)
        | StoredOptionsRefusal::CancelGraceInvalid(reason) => reason.as_str(),
    };
    error_envelope(TaskError::builtin(
        OperationalErrorCode::WorkerSerializationError,
        reason,
    ))
}

/// A run ready for its body, or the end of a run that cannot start one.
pub(super) enum Prepared {
    Body {
        task: RegisteredTask,
        input: Vec<u8>,
    },
    Ended(RunEnd),
}

/// Resolve the task and read its input. An unknown task is
/// `WORKER_RESOLUTION_ERROR`; an input the core's read rule refuses is
/// `WORKER_SERIALIZATION_ERROR`.
pub(super) fn prepare(app: &ChildApp, task_name: &str, input: &TaskInputText) -> Prepared {
    let task = match app.registry.get(task_name) {
        Ok(task) => task.clone(),
        Err(error) => {
            return Prepared::Ended(error_envelope(TaskError::builtin(
                OperationalErrorCode::WorkerResolutionError,
                format!("task '{task_name}' is not registered in this binary: {error}"),
            )))
        }
    };
    match build_task_input(input.as_str(), task.accepts_workflow_ctx()) {
        Ok(input) => Prepared::Body { task, input },
        Err(error) => Prepared::Ended(error_envelope(error)),
    }
}

/// The end of a body's result: the envelope. An ok value that is not JSON
/// is `WORKER_SERIALIZATION_ERROR` (the child checks its own result before
/// it sends it); an envelope over the frame body bound is a worker failure.
pub(super) fn result_end(result: TaskResult<Vec<u8>>) -> RunEnd {
    let end = match result {
        TaskResult::Ok(bytes) => match serde_json::from_slice::<&RawValue>(&bytes) {
            Ok(raw) => match serde_json::to_string(&TaskResult::Ok(raw)) {
                Ok(text) => RunEnd::Result(ResultEnvelopeText::new(text)),
                Err(error) => error_envelope(TaskError::builtin(
                    OperationalErrorCode::WorkerSerializationError,
                    format!("failed to encode the result envelope: {error}"),
                )),
            },
            Err(error) => error_envelope(TaskError::builtin(
                OperationalErrorCode::WorkerSerializationError,
                format!("failed to parse task result JSON: {error}"),
            )),
        },
        TaskResult::Err(error) => error_envelope(error),
    };
    let envelope_bytes = match &end {
        RunEnd::Result(envelope) => Some(envelope.as_str().len()),
        RunEnd::StoppedBeforeRunning(_) | RunEnd::WorkerFailure(_) => None,
    };
    match envelope_bytes {
        Some(bytes) if u64::try_from(bytes).unwrap_or(u64::MAX) > MAX_BODY_BYTES => {
            RunEnd::WorkerFailure(FailureDetail::cut(&format!(
                "the result envelope has {bytes} bytes; the frame bound is {MAX_BODY_BYTES}"
            )))
        }
        Some(_) | None => end,
    }
}

/// The body of a blocking task on the calling thread. A panic is
/// `UNHANDLED_EXCEPTION`.
pub(super) fn run_blocking_body(
    task: &Arc<dyn crate::core::task::fn_trait::BlockingTaskFn>,
    input: &[u8],
) -> TaskResult<Vec<u8>> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| task.execute(input))) {
        Ok(result) => result,
        Err(_panic) => TaskResult::Err(TaskError::builtin(
            OperationalErrorCode::UnhandledException,
            "blocking task panicked",
        )),
    }
}

/// The end of a task whose kind does not fit the seat it reached: an async
/// body on a thread seat, or a blocking body on an async seat. The claim
/// returns such a row before dispatch; here it is a worker failure.
pub(super) fn kind_mismatch_end(task_name: &str, seat: &str) -> RunEnd {
    RunEnd::WorkerFailure(FailureDetail::cut(&format!(
        "task '{task_name}' does not fit a {seat} seat"
    )))
}

//! One run in a Rust child: the start write, the refusal or the body, the
//! result envelope, the finalizing marker. The order is Python's (executor
//! design, section 4.8): start, user code, encode, marker, frame.

use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::value::RawValue;
use tracing::instrument::Instrumented;
use tracing::Instrument;
use uuid::Uuid;

use super::host::ChildApp;
use super::link::FrameWriter;
use super::panic::{catch_body_panic, panic_error, CatchPanic};
use super::role::as_blocking_body;
use super::run_work::{in_blocking_run, RunCancelSignal};
use crate::broker::child_writes::{WriteDeadline, PRE_START_CONFIRM_ATTEMPTS};
use crate::broker::claims::{
    release_claims_within, start_attempt as start_statement, ReleaseBatch, ReleaseClaims,
    ReleaseOutcome, ReleasePlacement, RunWorkflow, StartAttempt, StartMiss, StartOutcome,
};
use crate::broker::workflow_stop_before_start::stop_before_start;
use crate::broker::{BrokerError, OrphanSelfHeal, PostgresBroker, PrestartExpiry, StopBeforeStart};
use crate::core::codec::JsonText;
use crate::core::lifecycle::OwnedClaim;
use crate::core::task::error::{OperationalErrorCode, TaskError};
use crate::core::task::execution::{MAX_CANCEL_GRACE_MS, MIN_CANCEL_GRACE_MS};
use crate::core::task::fencing::FencingToken;
use crate::core::task::fn_trait::{RawTaskResult, RegisteredTask, TaskModule};
use crate::core::task::result::TaskResult;
use crate::events::{AttemptNumber, QueueName};
use crate::worker::execution::{
    build_task_input, mark_task_finalizing, phase_retry_delay, FinalizingMark,
};
use crate::worker::executor::protocol::{
    FailureDetail, ResultEnvelopeText, RunEnd, StopReason, StoredOptionsRefusal, TaskInputText,
    MAX_BODY_BYTES,
};
use crate::worker::retry::{MAX_TASK_TIMEOUT_MS, MIN_TASK_TIMEOUT_MS};

/// What every run of one child shares.
#[derive(Clone)]
pub(super) struct HostContext {
    pub(super) broker: Arc<PostgresBroker>,
    pub(super) app: Arc<ChildApp>,
    pub(super) pid: i32,
    pub(super) writer: FrameWriter,
}

/// The facts of one run, from its run frame: the task, the claim that every
/// write fences on (`worker_id`, `claimed_at`), and the queue and the
/// attempt that the span of its body names.
#[derive(Debug, Clone)]
pub(super) struct RunFacts {
    pub(super) task_id: Uuid,
    pub(super) task_name: String,
    pub(super) queue_name: QueueName,
    pub(super) attempt: AttemptNumber,
    pub(super) worker_id: String,
    pub(super) claimed_at: DateTime<Utc>,
    /// Whether the stop before start cancels the task when no node links it
    /// live; the run frame carries it.
    pub(super) orphan_self_heal: OrphanSelfHeal,
}

impl RunFacts {
    /// The fencing token of the run: the task id and the attempt number.
    pub(super) fn fencing_token(&self) -> FencingToken {
        FencingToken::new(self.task_id, self.attempt)
    }
}

/// How the start write ended.
pub(super) enum Started {
    /// The row is RUNNING; `started_at` is the time that the start write
    /// returned (the database clock), and `workflow` the node that links
    /// the task, read with the node row that the start locked.
    Running {
        started_at: DateTime<Utc>,
        workflow: RunWorkflow,
    },
    Stopped(StopReason),
}

/// Sends of the release of a claim after a database error of the start, the
/// first included.
const START_RELEASE_SENDS: u32 = 3;

/// What the path of a missed start ended with.
enum MissedStart {
    /// The run ends before any task code, with this reason.
    Stop(StopReason),
    /// The workflow was resumed before the abandon: start the task again.
    WorkflowResumed,
}

/// The start write (Python `_confirm_until_not_resumed`,
/// `_confirm_ownership_and_set_running`): one `horsies_start_attempt`
/// statement sets the row RUNNING under the claim fence with its first
/// runner beat and, for a workflow task, its node. A miss runs the path of
/// its reason (`after_missed_start`) and the run ends before any task code.
/// A workflow resumed before the abandon starts the task again, at most
/// [`PRE_START_CONFIRM_ATTEMPTS`] times; then the claim is left to its lease
/// (`OWNERSHIP_UNCONFIRMED`).
///
/// Each attempt has its deadline ([`WriteDeadline::start_attempt`], Python
/// `RUNNING_ATTEMPT_TIMEOUT_S`): the start, its miss path and the release
/// below end by it, and a statement past it gets a cancel request, so the
/// start does not commit after the bound. One attempt takes at most
/// [`crate::broker::child_writes::START_ATTEMPT_END`].
///
/// A database error of the start or of its miss path, a cancel at the bound
/// included, releases the claim with the fenced release in the time left of
/// the attempt, and the run ends `OWNERSHIP_UNCONFIRMED`.
pub(super) async fn start_attempt(context: &HostContext, facts: &RunFacts) -> Started {
    let command = StartAttempt::of_process(
        facts.task_id,
        OwnedClaim {
            worker_id: facts.worker_id.clone(),
            claimed_at: facts.claimed_at,
        },
        context.pid,
        context.app.hostname.clone(),
    );
    for _ in 0..PRE_START_CONFIRM_ATTEMPTS {
        let deadline = WriteDeadline::start_attempt();
        let outcome = match context.broker.core_pools_for_write() {
            Ok((open, _write)) => {
                start_statement(
                    open.main(),
                    context.broker.statement_log(),
                    &command,
                    deadline,
                )
                .await
            }
            Err(error) => Err(error),
        };
        let missed = match outcome {
            Ok(StartOutcome::Started {
                started_at,
                workflow,
            }) => {
                return Started::Running {
                    started_at,
                    workflow,
                }
            }
            Ok(StartOutcome::Missed(miss)) => {
                after_missed_start(context, facts, miss, deadline).await
            }
            Err(error) => Err(error),
        };
        match missed {
            Ok(MissedStart::Stop(reason)) => return Started::Stopped(reason),
            Ok(MissedStart::WorkflowResumed) => continue,
            Err(error) => {
                tracing::error!(
                    task_id = %facts.task_id,
                    "Failed to transition task {} to RUNNING: {error}",
                    facts.task_id,
                );
                release_after_start_error(context, facts, deadline).await;
                return Started::Stopped(StopReason::OwnershipUnconfirmed);
            }
        }
    }
    tracing::warn!(
        task_id = %facts.task_id,
        "Task {} workflow was resumed on each of {PRE_START_CONFIRM_ATTEMPTS} start attempts; \
         leaving the claim to its lease",
        facts.task_id,
    );
    Started::Stopped(StopReason::OwnershipUnconfirmed)
}

/// The path of a start that missed, by its reason (Python
/// `_after_missed_start`):
///
/// | Reason | Path | Stop |
/// |---|---|---|
/// | `DeadlinePassed` | the expiry of the owned claim | `TaskExpired`, or `ClaimLost` when the expiry is refused |
/// | `NotHeld`, `LeaseExpired` | none | `ClaimLost` |
/// | `WorkflowPaused`, `WorkflowCancelled`, `NodeNotLinked` | `horsies_workflow_task_stop_before_start` with the orphan self-heal of the run, one statement that judges the task again: the abandon and node reset, the cancel and node skip, or the orphan cancel | `WorkflowStopped`; a new start after a resume; `TaskExpired`, `OrphanCancelled`, `NodeNotLinked` or `ClaimLost` by what it found |
///
/// Each write of the path ends by `deadline`, the deadline of the start
/// attempt.
///
/// Errors: the [`BrokerError`] of a write of the path; a write whose
/// deadline passed first sends nothing.
async fn after_missed_start(
    context: &HostContext,
    facts: &RunFacts,
    miss: StartMiss,
    deadline: WriteDeadline,
) -> Result<MissedStart, BrokerError> {
    match miss {
        StartMiss::DeadlinePassed => {
            match context
                .broker
                .expire_claimed_task_before_start(facts.task_id, &facts.worker_id, deadline)
                .await?
            {
                PrestartExpiry::Expired => {
                    tracing::info!(
                        task_id = %facts.task_id,
                        "Task {} expired before actual execution start; marked EXPIRED.",
                        facts.task_id,
                    );
                    Ok(MissedStart::Stop(StopReason::TaskExpired))
                }
                PrestartExpiry::NotExpired => Ok(claim_lost(facts)),
            }
        }
        StartMiss::NotHeld | StartMiss::LeaseExpired => Ok(claim_lost(facts)),
        StartMiss::WorkflowPaused | StartMiss::WorkflowCancelled | StartMiss::NodeNotLinked => {
            stop_for_workflow(context, facts, miss, deadline).await
        }
    }
}

/// The stop of a start whose claim is gone. Logs a warning (Python
/// `_claim_lost`).
fn claim_lost(facts: &RunFacts) -> MissedStart {
    tracing::warn!(
        task_id = %facts.task_id,
        "Task {} ownership lost - claim was reclaimed or status changed. Aborting execution to \
         prevent double-execution.",
        facts.task_id,
    );
    MissedStart::Stop(StopReason::ClaimLost)
}

/// Stop a workflow task whose start missed for a workflow reason (its
/// workflow stopped, or no node links it): one
/// `horsies_workflow_task_stop_before_start` statement on the main core
/// pool, as the start, within `deadline`, which judges the task again under
/// its own locks. With the orphan self-heal of the run on, a task that no
/// node links live is cancelled in the same statement (`OrphanCancelled`);
/// with it off the task ends `NodeNotLinked` and its row stays CLAIMED.
async fn stop_for_workflow(
    context: &HostContext,
    facts: &RunFacts,
    miss: StartMiss,
    deadline: WriteDeadline,
) -> Result<MissedStart, BrokerError> {
    tracing::info!(
        task_id = %facts.task_id,
        miss = miss.as_str(),
        "Blocking task {} execution before start - its workflow stopped",
        facts.task_id,
    );
    let fence = OwnedClaim {
        worker_id: facts.worker_id.clone(),
        claimed_at: facts.claimed_at,
    };
    let (open, _write) = context.broker.core_pools_for_write()?;
    let stop = stop_before_start(
        open.main(),
        context.broker.statement_log(),
        facts.task_id,
        &fence,
        facts.orphan_self_heal,
        deadline,
    )
    .await?;
    match stop {
        StopBeforeStart::Abandoned(_) | StopBeforeStart::Cancelled(_) => {
            Ok(MissedStart::Stop(StopReason::WorkflowStopped))
        }
        StopBeforeStart::Resumed => Ok(MissedStart::WorkflowResumed),
        StopBeforeStart::Expired(_) => {
            tracing::info!(
                task_id = %facts.task_id,
                "Task {} expired before actual execution start; marked EXPIRED.",
                facts.task_id,
            );
            Ok(MissedStart::Stop(StopReason::TaskExpired))
        }
        StopBeforeStart::OrphanCancelled(_) => Ok(MissedStart::Stop(StopReason::OrphanCancelled)),
        StopBeforeStart::NodeNotLinked => Ok(node_not_linked(facts)),
        StopBeforeStart::NotHeld(_) => Ok(claim_lost(facts)),
    }
}

/// The stop of a workflow task start whose node is not linked to the task,
/// with the orphan self-heal off: the stop wrote nothing, and the row stays
/// CLAIMED for inspection. Logs an error (Python
/// `clients/python/horsies/core/worker/child_runner.py`, the node check of
/// `_confirm_ownership_and_set_running`).
fn node_not_linked(facts: &RunFacts) -> MissedStart {
    tracing::error!(
        task_id = %facts.task_id,
        "Failed to update workflow_tasks to RUNNING for task {}",
        facts.task_id,
    );
    MissedStart::Stop(StopReason::NodeNotLinked)
}

/// Release the claim of a start that failed with a database error, at once,
/// with the fenced release in its class (owner rule "Database error at the
/// start"). A row the start set RUNNING before its reply was lost answers
/// `NotHeld` and is left to stale recovery. An error that may clear sends the
/// release again, at most [`START_RELEASE_SENDS`] sends in all.
///
/// Every send ends by `deadline`, the deadline of the start attempt, and
/// no send or wait starts past it. A release that fails, or that has no
/// time left, leaves the claim to the worker: its outcome writer releases
/// the claim of an `OWNERSHIP_UNCONFIRMED` stop, and the claim lease
/// returns the row otherwise.
async fn release_after_start_error(
    context: &HostContext,
    facts: &RunFacts,
    deadline: WriteDeadline,
) {
    let batch = match ReleaseBatch::new(&[(facts.task_id, facts.claimed_at)]) {
        Ok(batch) => batch,
        Err(refused) => unreachable!("one claim is a valid release batch: {refused}"),
    };
    let command = ReleaseClaims {
        worker_id: facts.worker_id.clone(),
        batch,
        placement: ReleasePlacement::SameClass,
    };
    for send in 1..=START_RELEASE_SENDS {
        let sent = match context.broker.core_pools_for_write() {
            Ok((open, _write)) => {
                release_claims_within(
                    open.main(),
                    context.broker.statement_log(),
                    &command,
                    deadline,
                )
                .await
            }
            Err(error) => Err(error),
        };
        match sent {
            Ok(outcomes) => {
                let released = matches!(outcomes.as_slice(), [(_, ReleaseOutcome::Released)]);
                tracing::warn!(
                    task_id = %facts.task_id,
                    released,
                    "the claim of a start that failed went back to the queue when held",
                );
                return;
            }
            Err(error) if error.is_retryable() && send < START_RELEASE_SENDS => {
                let delay = std::time::Duration::from_secs_f64(phase_retry_delay(send));
                match deadline.left() {
                    Some(left) if left > delay => {
                        tracing::warn!(
                            task_id = %facts.task_id,
                            send,
                            error = %error,
                            "the release after a failed start failed: {error}; sending it again",
                        );
                        tokio::time::sleep(delay).await;
                    }
                    Some(_) | None => {
                        tracing::error!(
                            task_id = %facts.task_id,
                            send,
                            error = %error,
                            "the release after a failed start failed and the start attempt has \
                             no time left: {error}; the worker releases the claim",
                        );
                        return;
                    }
                }
            }
            Err(error) => {
                tracing::error!(
                    task_id = %facts.task_id,
                    error = %error,
                    "the release after a failed start failed: {error}; the worker releases the \
                     claim",
                );
                return;
            }
        }
    }
}

/// The finalizing marker of a run that set its row RUNNING, within
/// [`crate::broker::child_writes::FINALIZING_MARKER_BOUND`] (Python
/// `FINALIZING_MARKER_TIMEOUT_S`): past it the marker is cancelled and not
/// written later, and the call ends within
/// [`crate::broker::child_writes::FINALIZING_MARKER_END`]. A failed write is
/// logged: the parent's write is fenced on its own.
pub(super) async fn mark_finalizing(context: &HostContext, facts: &RunFacts) {
    match mark_task_finalizing(
        &context.broker,
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
            tracing::warn!(
                task_id = %facts.task_id,
                error = %error,
                "Failed to mark task {} finalizing: {error}",
                facts.task_id,
            );
        }
    }
}

/// The envelope of a task error.
fn error_envelope(error: TaskError) -> RunEnd {
    // The codec's encoder writes each float of `data` as Python's
    // `json.dumps` does.
    let text = crate::core::codec::task_result_to_json_string(
        &TaskResult::<serde_json::Value>::Err(error),
    )
    .expect("a task error holds only JSON values, so its envelope encodes");
    RunEnd::Result(ResultEnvelopeText::new(text))
}

/// The end of a run of task `task_name` whose stored options the worker
/// cannot use: no task code runs, and the task fails with
/// `WORKER_SERIALIZATION_ERROR`. The message is the reason. `data` holds
/// `task_name` and the stored `field` that the worker cannot use, and for
/// `timeout_ms` and `cancel_grace_ms` the bounds `min_ms` and `max_ms`, in
/// this order (Python `_stored_timeout_refusal_response`). Pure.
pub(super) fn refusal_end(task_name: &str, refusal: &StoredOptionsRefusal) -> RunEnd {
    let (reason, data) = match refusal {
        StoredOptionsRefusal::OptionsUnreadable(reason) => (
            reason,
            serde_json::json!({ "task_name": task_name, "field": "task_options" }),
        ),
        StoredOptionsRefusal::TimeoutValueInvalid(reason) => (
            reason,
            serde_json::json!({
                "task_name": task_name,
                "field": "timeout_ms",
                "min_ms": MIN_TASK_TIMEOUT_MS,
                "max_ms": MAX_TASK_TIMEOUT_MS,
            }),
        ),
        StoredOptionsRefusal::CancelGraceInvalid(reason) => (
            reason,
            serde_json::json!({
                "task_name": task_name,
                "field": "cancel_grace_ms",
                "min_ms": MIN_CANCEL_GRACE_MS,
                "max_ms": MAX_CANCEL_GRACE_MS,
            }),
        ),
    };
    let mut error = TaskError::built_in(
        OperationalErrorCode::WorkerSerializationError,
        reason.as_str(),
    );
    error.data = Some(JsonText::of_engine_value(&data));
    error_envelope(error)
}

/// A run ready for its body, or the end of a run that cannot start one.
pub(super) enum Prepared {
    Body {
        task: Box<RegisteredTask>,
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
            return Prepared::Ended(error_envelope(TaskError::built_in(
                OperationalErrorCode::WorkerResolutionError,
                format!("task '{task_name}' is not registered in this binary: {error}"),
            )))
        }
    };
    match build_task_input(input.as_str(), task.accepts_workflow_context()) {
        Ok(input) => Prepared::Body {
            task: Box::new(task),
            input,
        },
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
                Err(error) => error_envelope(TaskError::built_in(
                    OperationalErrorCode::WorkerSerializationError,
                    format!("failed to encode the result envelope: {error}"),
                )),
            },
            Err(error) => error_envelope(TaskError::built_in(
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

/// The body of blocking task `task_name` (defined in `module`) on the
/// calling thread, inside `span` (the span of the run's body scope), with
/// `token` as the fencing token and `cancel` as the cancel signal of the
/// thread while the body runs ([`in_blocking_run`]): the body reads
/// `run_cancel_state` and `run_cancel_signal` there. A panic is
/// `UNHANDLED_EXCEPTION` with its record ([`panic_error`]).
pub(super) fn run_blocking_body(
    task: &Arc<dyn crate::core::task::fn_trait::BlockingTaskFn>,
    input: &[u8],
    task_name: &str,
    module: TaskModule,
    span: &tracing::Span,
    token: FencingToken,
    cancel: RunCancelSignal,
) -> TaskResult<Vec<u8>> {
    let body = || in_blocking_run(token, cancel, || as_blocking_body(|| task.execute(input)));
    match span.in_scope(|| catch_body_panic(body)) {
        Ok(result) => result,
        Err(record) => TaskResult::Err(panic_error(task_name, module, record)),
    }
}

/// The async body of `task` on `input`, polled inside `span` (the span of
/// the run's body scope). A panic is caught in the poll that panicked
/// ([`CatchPanic`]); the caller maps it.
pub(super) fn async_body_in(
    task: Arc<dyn crate::core::task::fn_trait::AsyncTaskFn>,
    input: Vec<u8>,
    span: tracing::Span,
) -> CatchPanic<Instrumented<impl Future<Output = RawTaskResult> + Send>> {
    CatchPanic::new(async move { task.execute(&input).await }.instrument(span))
}

/// The end of a task whose kind does not fit the seat it reached: an async
/// body on a thread seat, or a blocking body on an async seat. The claim
/// returns such a row before dispatch; here it is a worker failure.
pub(super) fn kind_mismatch_end(task_name: &str, seat: &str) -> RunEnd {
    RunEnd::WorkerFailure(FailureDetail::cut(&format!(
        "task '{task_name}' does not fit a {seat} seat"
    )))
}

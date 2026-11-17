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
//! | `Reported(stopped before running)` | none; an expired workflow task replays phase 2; an unconfirmed start releases the claim (`horsies_release_claims` with placement `SAME_CLASS`); a task whose node is not linked is cancelled as an owned orphan (`horsies_cancel_owned_orphan`) when the app's `auto_terminate_orphaned_workflow_tasks` is on |
//! | `Reported(worker failure)` | the failed attempt, `BROKER_ERROR`, no retry |
//! | `TimedOut`, `KilledForLoop`, `Crashed` | the failed attempt with `TASK_TIMEOUT`, the loop code or `WORKER_CRASHED`, the retry the policy admits, at the placement of the end, under the claimed-row rule `RELEASE`: a row that is still CLAIMED goes back to the queue in the same statement |
//! | `EndUnknown` | none: stale recovery or the lease decides the row |
//!
//! A database error never decides between a retry and a failure: the write
//! is sent again, at most the retries of its [`WriteStage`]. After the last
//! retry of a `TASK_TIMEOUT` write, crash recovery writes `TASK_TIMEOUT` at
//! the same placement, with its own retries (Python
//! `_recover_timed_out_row`). A plain success sent again joins a later
//! batch; a row whose batch committed before its reply was lost answers
//! `AlreadyApplied`.

use std::sync::Arc;
use std::time::Duration;

use serde_json::value::RawValue;
use uuid::Uuid;

use super::dispatch::{AttemptEnd, ClaimedTask, Settlement, TimeoutCancel};
use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, RetryPlacement, RetryRequest,
};
use crate::broker::claims::{
    release_claims, ReleaseBatch, ReleaseClaims, ReleaseOutcome, ReleasePlacement,
};
use crate::broker::terminalization::terminalize;
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::config::payload::{enforce_payload_policy, PayloadKind, PayloadPolicy};
use crate::core::config::retention::RetentionConfig;
use crate::core::lifecycle::{OwnedClaim, TerminalizationCommand, TerminalizationOutcome};
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::task::error::{BuiltInTaskCode, OperationalErrorCode, OutcomeCode, TaskError};
use crate::core::task::result::{read_task_result_text, TaskResult, TaskResultText};
use crate::db::Transience;
use crate::worker::execution::{
    persist_failed_attempt, persist_ok_result, persist_undecodable_result,
    persist_unreported_attempt, phase_retry_delay, run_phase2, FinalizeError, FinalizeOutcome,
    FinalizeStage, Phase2Work, PhaseTwoForm, UnreportedAttemptWrite, PHASE1_MAX_RETRIES,
};
use crate::worker::executor::protocol::{ResultEnvelopeText, RunEnd, StopReason, TaskTimeoutMs};
use crate::worker::finalize_batch::statement::WorkerPoolSuccessWrite;
use crate::worker::finalize_batch::{FinalizeBatcher, PlainSuccess, RowNotWritten};
use crate::worker::retry::{admit_retry, RetryAdmission};

/// The text of a worker failure with no detail (Python
/// `_WORKER_FAILURE_FALLBACK`).
const WORKER_FAILURE_FALLBACK: &str = "Worker failure";

/// Retries of a `TASK_TIMEOUT` write (Python `_FINALIZE_TIMEOUT_MAX_RETRIES`).
pub const TIMEOUT_WRITE_MAX_RETRIES: u32 = 3;
/// Retries of a crash-recovery write (Python `_FINALIZE_FUTURE_MAX_RETRIES`).
pub const CRASH_RECOVERY_MAX_RETRIES: u32 = 3;
/// Sends of the release of a claim that no child took, the first included.
pub const RELEASE_SENDS: u32 = 3;

/// Whether the worker cancels a claimed workflow task whose node is not
/// linked to it (the app's `auto_terminate_orphaned_workflow_tasks`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanSelfHeal {
    /// Cancel the task with `horsies_cancel_owned_orphan`.
    On,
    /// Leave the row CLAIMED for inspection.
    Off,
}

impl OrphanSelfHeal {
    /// The setting of `auto_terminate_orphaned_workflow_tasks`.
    pub fn of_recovery_flag(auto_terminate_orphaned_workflow_tasks: bool) -> Self {
        match auto_terminate_orphaned_workflow_tasks {
            true => Self::On,
            false => Self::Off,
        }
    }
}

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
    Success {
        value: Vec<u8>,
    },
    Undecodable {
        diagnosis: String,
    },
    Failure(FinishFailedAttempt),
    /// The cancel of a claimed workflow task whose node is not linked.
    CancelOrphan,
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
    error.data = Some(serde_json::json!({ "task_id": task.task_id.to_string() }));
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
/// `_recover_worker_future_failure`). Pure.
pub fn killed_child_write(
    task: &ClaimedTask,
    worker_id: &str,
    code: BuiltInTaskCode,
    message: String,
    placement: RetryPlacement,
) -> FinishFailedAttempt {
    let code_text = code.value();
    let mut error = TaskError::builtin(code, message.clone());
    error.data = Some(serde_json::json!({
        "task_id": task.task_id.to_string(),
        "worker_id": worker_id,
    }));
    FinishFailedAttempt {
        task_id: task.task_id,
        fence: fence(task, worker_id),
        retry: retry_request(task, Some(&code_text), placement),
        result_json: error_envelope(error),
        error_code: Some(code_text),
        failed_reason: None,
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some(message),
            failed_reason: None,
        },
    }
}

/// The `TASK_TIMEOUT` write of an attempt that reached `timeout`, with
/// Python's error `data`: `task_id`, `timeout_ms`, and, for a shared async
/// run that its child ended, `cancel_end` (Python `_settle_timed_out_row`).
/// Pure.
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
    error.data = Some(timeout_data(task.task_id, timeout, cancel));
    FinishFailedAttempt {
        task_id: task.task_id,
        fence: fence(task, worker_id),
        retry: retry_request(task, Some(&code_text), placement),
        result_json: error_envelope(error),
        error_code: Some(code_text),
        failed_reason: None,
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some(message),
            failed_reason: None,
        },
    }
}

/// The `data` of a `TASK_TIMEOUT` error. `cancel_end` is set only for a
/// shared async run that its child ended. Pure.
fn timeout_data(task_id: Uuid, timeout: TaskTimeoutMs, cancel: TimeoutCancel) -> serde_json::Value {
    let mut data = serde_json::json!({
        "task_id": task_id.to_string(),
        "timeout_ms": timeout.get(),
    });
    match cancel {
        TimeoutCancel::Ended(end) => {
            data["cancel_end"] = serde_json::Value::from(end.as_str());
        }
        TimeoutCancel::NoSignal | TimeoutCancel::EndedByKill => {}
    }
    data
}

/// The crash-recovery write of an attempt that reached `timeout` and whose
/// `TASK_TIMEOUT` write failed on every retry: the same code and placement,
/// with a message that names the failed writes, and with the `data` of
/// crash recovery (Python `_recover_timed_out_row`). Pure.
pub fn timeout_recovery_write(
    task: &ClaimedTask,
    worker_id: &str,
    timeout: TaskTimeoutMs,
    placement: RetryPlacement,
) -> FinishFailedAttempt {
    killed_child_write(
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
            Settlement::Written
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
        "DB error while requeueing task {} ({reason}); its claim lease returns the row",
        task.task_id,
    );
    match retryable {
        true => Settlement::RetriesExhausted,
        false => Settlement::WriteFailed,
    }
}

/// Writes the end of each attempt with the existing statements.
///
/// The plain successes go to the finalize batcher of the worker, which
/// writes them in batches; the worker stop closes it after the drain of the
/// outcome writes (`finalize_batch::stop::drain_then_close`).
pub struct OutcomeWriter {
    broker: Arc<PostgresBroker>,
    workflows: Arc<WorkflowSpecRegistry>,
    worker_id: String,
    payload_policy: PayloadPolicy,
    retention: RetentionConfig,
    successes: FinalizeBatcher<WorkerPoolSuccessWrite>,
    orphan_self_heal: OrphanSelfHeal,
}

impl OutcomeWriter {
    pub fn new(
        broker: Arc<PostgresBroker>,
        workflows: Arc<WorkflowSpecRegistry>,
        worker_id: String,
        payload_policy: PayloadPolicy,
        retention: RetentionConfig,
        successes: FinalizeBatcher<WorkerPoolSuccessWrite>,
        orphan_self_heal: OrphanSelfHeal,
    ) -> Self {
        Self {
            broker,
            workflows,
            worker_id,
            payload_policy,
            retention,
            successes,
            orphan_self_heal,
        }
    }

    /// Write the end of one attempt, then phase 2 when it is owed.
    pub async fn settle(&self, task: &ClaimedTask, end: AttemptEnd) -> Settlement {
        match end {
            AttemptEnd::NotDispatched(cause) => self.release(task, &cause.to_string()).await,
            AttemptEnd::EndUnknown { pid } => {
                tracing::warn!(
                    task_id = %task.task_id,
                    pid = pid.get(),
                    "the exit of the task's child was not seen; recovery decides the row",
                );
                Settlement::LeftToRecovery
            }
            AttemptEnd::Reported(RunEnd::StoppedBeforeRunning(reason)) => {
                self.after_stop(task, reason).await
            }
            AttemptEnd::Reported(RunEnd::Result(envelope)) => {
                let write = self.result_write(task, &envelope);
                self.phase_one(task, write, ClaimedRowRule::Refuse, WriteStage::PhaseOne)
                    .await
            }
            AttemptEnd::Reported(RunEnd::WorkerFailure(detail)) => {
                let write = worker_failure_write(task, &self.worker_id, detail.as_str());
                self.phase_one(
                    task,
                    PhaseOneWrite::Failure(write),
                    ClaimedRowRule::Refuse,
                    WriteStage::PhaseOne,
                )
                .await
            }
            AttemptEnd::TimedOut {
                timeout,
                retry,
                cancel,
            } => self.timed_out(task, timeout, retry, cancel).await,
            AttemptEnd::KilledForLoop {
                code,
                retry,
                reason,
            } => {
                let write = killed_child_write(
                    task,
                    &self.worker_id,
                    code.code().into(),
                    reason.into_string(),
                    retry,
                );
                self.phase_one(
                    task,
                    PhaseOneWrite::Failure(write),
                    ClaimedRowRule::Release,
                    WriteStage::CrashRecovery,
                )
                .await
            }
            AttemptEnd::Crashed { reason } => {
                let write = killed_child_write(
                    task,
                    &self.worker_id,
                    OperationalErrorCode::WorkerCrashed.into(),
                    reason.into_string(),
                    RetryPlacement::SameClass,
                );
                self.phase_one(
                    task,
                    PhaseOneWrite::Failure(write),
                    ClaimedRowRule::Release,
                    WriteStage::CrashRecovery,
                )
                .await
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
            Settlement::Written
            | Settlement::NothingOwed
            | Settlement::NotHeld
            | Settlement::LeftToRecovery
            | Settlement::WriteFailed => return settled,
        }
        tracing::error!(
            task_id = %task.task_id,
            "every retry of the TASK_TIMEOUT write failed; crash recovery writes the row",
        );
        let recovery = timeout_recovery_write(task, &self.worker_id, timeout, placement);
        self.phase_one(
            task,
            PhaseOneWrite::Failure(recovery),
            ClaimedRowRule::Release,
            WriteStage::CrashRecovery,
        )
        .await
    }

    fn result_write(&self, task: &ClaimedTask, envelope: &ResultEnvelopeText) -> PhaseOneWrite {
        // Warn-only: a result is never refused over size; the work is done.
        enforce_payload_policy(
            &self.payload_policy,
            &task.task_name,
            PayloadKind::Result,
            envelope.as_str().len(),
        );
        match read_task_result_text(envelope.as_str()) {
            Ok(TaskResultText::Ok(value)) => PhaseOneWrite::Success {
                value: value.get().as_bytes().to_vec(),
            },
            Ok(TaskResultText::Err(error)) => {
                PhaseOneWrite::Failure(task_error_write(task, &self.worker_id, &error, envelope))
            }
            Err(refused) => PhaseOneWrite::Undecodable {
                diagnosis: refused.to_string(),
            },
        }
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
            (PhaseOneWrite::CancelOrphan, _) => self.cancel_orphan(task).await,
        };
        written.map(PhaseOneOutcome::Finalize)
    }

    /// Cancel a claimed workflow task whose node is not linked to it, fenced
    /// to this claim (Python `python/horsies/core/worker/finalize.py:1216-1260`,
    /// the `CancelOwnedOrphan` decision of `_persist_task_terminal_state`).
    /// The function judges the link again under its own lock: a task that
    /// is no longer a terminable owned orphan is left as it is. Nothing is
    /// owed to phase 2 in either case.
    ///
    /// Errors: a [`FinalizeError`] of phase 1, retryable when the database
    /// error may clear.
    async fn cancel_orphan(&self, task: &ClaimedTask) -> Result<FinalizeOutcome, FinalizeError> {
        let command = TerminalizationCommand::CancelOwnedOrphan {
            task_id: task.task_id,
            fence: fence(task, &self.worker_id),
        };
        let cancelled = match self.broker.core_pools_for_write() {
            Ok((open, _write)) => {
                terminalize(open.main(), self.broker.statement_log(), &command).await
            }
            Err(error) => Err(error),
        };
        let outcomes = cancelled.map_err(|error| FinalizeError {
            stage: FinalizeStage::Phase1Persist,
            task_id: task.task_id,
            message: format!("Failed to terminate orphaned workflow task: {error}"),
            retryable: error.is_retryable(),
        })?;
        match outcomes.first() {
            Some(TerminalizationOutcome::Applied { .. }) => tracing::info!(
                task_id = %task.task_id,
                "Task {} cancelled: orphaned workflow task (no live workflow_task linkage)",
                task.task_id,
            ),
            Some(
                TerminalizationOutcome::AlreadyApplied { .. }
                | TerminalizationOutcome::LostClaim { .. }
                | TerminalizationOutcome::SourceStateConflict { .. }
                | TerminalizationOutcome::TaskAbsent { .. },
            )
            | None => tracing::debug!(
                task_id = %task.task_id,
                "Task {} was no longer a terminable owned orphan",
                task.task_id,
            ),
        }
        Ok(FinalizeOutcome::Finalized)
    }

    /// The success of a plain task: its row joins the next batch of the
    /// finalize batcher (Python `_complete_plain_success`,
    /// `python/horsies/core/worker/finalize.py:1386-1436`). Returns after the
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

    /// The write and at most the retries of its `stage`; then phase 2 when
    /// owed.
    ///
    /// The write is boxed: its future holds every writer of phase 1, and
    /// `settle` builds this future at seven call sites. Inline, a debug
    /// build needs more than a 2 MiB thread stack for one settlement.
    async fn phase_one(
        &self,
        task: &ClaimedTask,
        write: PhaseOneWrite,
        claimed_row: ClaimedRowRule,
        stage: WriteStage,
    ) -> Settlement {
        let max_retries = stage.max_retries();
        let mut retries_done: u32 = 0;
        let outcome = loop {
            let error = match Box::pin(self.write_once(task, &write, claimed_row)).await {
                Ok(outcome) => break outcome,
                Err(error) => error,
            };
            match (error.retryable, retries_done < max_retries) {
                (true, true) => {
                    retries_done += 1;
                    tracing::warn!(
                        task_id = %task.task_id,
                        stage = ?stage,
                        retry = retries_done,
                        max = max_retries,
                        error = %error,
                        "the outcome write failed; sending it again",
                    );
                    tokio::time::sleep(Duration::from_secs_f64(phase_retry_delay(retries_done)))
                        .await;
                }
                (true, false) => {
                    tracing::error!(
                        task_id = %task.task_id,
                        stage = ?stage,
                        retries = max_retries,
                        error = %error,
                        "every retry of the outcome write failed",
                    );
                    return Settlement::RetriesExhausted;
                }
                (false, _) => {
                    tracing::error!(
                        task_id = %task.task_id,
                        stage = ?stage,
                        error = %error,
                        "the outcome write failed with an error no retry can clear; the row \
                         waits for recovery",
                    );
                    return Settlement::WriteFailed;
                }
            }
        };
        match outcome {
            PhaseOneOutcome::Finalize(FinalizeOutcome::Terminal {
                capacity_notified,
                phase_two,
            }) => {
                self.phase_two(task, capacity_notified, phase_two).await;
                Settlement::Written
            }
            PhaseOneOutcome::Finalize(FinalizeOutcome::Retried | FinalizeOutcome::Finalized) => {
                Settlement::Written
            }
            PhaseOneOutcome::Finalize(FinalizeOutcome::Refused) => Settlement::NotHeld,
            PhaseOneOutcome::ClaimReleased => {
                tracing::warn!(
                    task_id = %task.task_id,
                    "the child was killed before the task ran; the claim went back to the queue",
                );
                Settlement::Written
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
            &self.workflows,
            Phase2Work {
                task_id: task.task_id,
                queue_name: task.queue_name.clone(),
                is_workflow_task: task.kind.is_workflow_task(),
                capacity_notified,
                phase_two,
            },
            &self.retention,
        ))
        .await;
    }

    /// Release a claim that no child took (Python `_requeue_claimed_task`,
    /// `python/horsies/core/worker/dispatch.py:254-295`): `horsies_release_claims`
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
                        "the release of the claim failed; sending it again",
                    );
                    tokio::time::sleep(Duration::from_secs_f64(phase_retry_delay(sends))).await;
                    sends += 1;
                }
                (retryable, _) => return release_failed(task, reason, &error, retryable),
            }
        }
    }

    /// The child stopped before RUNNING; no task code ran (Python
    /// `python/horsies/core/lifecycle/classify.py:260-300`, `_classify_abort`).
    ///
    /// - An expired workflow task replays phase 2.
    /// - An unconfirmed start releases the claim at once with the fenced
    ///   release (Python `ReleaseUnconfirmedClaim`): the row goes back to the
    ///   queue with its class only while it is CLAIMED by this worker at this
    ///   claim generation. A row outside the fence, such as a start that
    ///   committed before its reply was lost, is left to recovery. A release
    ///   that fails is logged, and the claim lease returns the row.
    /// - A task whose node is not linked is cancelled as an owned orphan when
    ///   the app turns the self-heal on; with it off the row stays CLAIMED
    ///   for inspection.
    /// - Every other stop wrote what its case needs in the child; an
    ///   orphan that the child's stop function cancelled owes nothing more.
    async fn after_stop(&self, task: &ClaimedTask, reason: StopReason) -> Settlement {
        match (reason, task.kind.is_workflow_task(), self.orphan_self_heal) {
            (StopReason::TaskExpired, true, _) => {
                self.phase_two(task, false, PhaseTwoForm::NotKnown).await;
                Settlement::NothingOwed
            }
            (StopReason::OwnershipUnconfirmed, _, _) => {
                self.release(task, "the start of the attempt was not confirmed")
                    .await
            }
            (StopReason::NodeNotLinked, _, OrphanSelfHeal::On) => {
                self.phase_one(
                    task,
                    PhaseOneWrite::CancelOrphan,
                    ClaimedRowRule::Refuse,
                    WriteStage::PhaseOne,
                )
                .await
            }
            (StopReason::NodeNotLinked, _, OrphanSelfHeal::Off) => {
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
                _,
            ) => Settlement::NothingOwed,
        }
    }
}

//! The hand-off from an [`AttemptEnd`] to the existing outcome writers
//! (Python executor design, section 4.7). One end gives one statement, then
//! phase 2 when the statement owes it.
//!
//! | End | Statement |
//! |---|---|
//! | `NotDispatched` | release of the claim; the row goes back to the queue |
//! | `Reported(result)`, a value | the fused success of a plain task, or the workflow success |
//! | `Reported(result)`, a task error | the failed attempt, with the retry the policy admits for its code |
//! | `Reported(result)` that does not read as the envelope | the failed attempt, `WORKER_SERIALIZATION_ERROR`, no retry |
//! | `Reported(stopped before running)` | none; an expired workflow task replays phase 2 |
//! | `Reported(worker failure)` | the failed attempt, `BROKER_ERROR`, no retry |
//! | `TimedOut`, `KilledForLoop`, `Crashed` | the failed attempt with `TASK_TIMEOUT`, the loop code or `WORKER_CRASHED`, the retry the policy admits, at the placement of the end; a row that is still CLAIMED is released |
//! | `EndUnknown` | none: stale recovery or the lease decides the row |
//!
//! A database error never decides between a retry and a failure: the write
//! is sent again, at most [`PHASE1_MAX_RETRIES`] more times.

use std::sync::Arc;
use std::time::Duration;

use super::dispatch::{AttemptEnd, ClaimedTask, Settlement};
use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, RetryPlacement, RetryRequest,
};
use crate::broker::PostgresBroker;
use crate::core::config::payload::{enforce_payload_policy, PayloadKind, PayloadPolicy};
use crate::core::config::retention::RetentionConfig;
use crate::core::lifecycle::OwnedClaim;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::task::error::{BuiltInTaskCode, OperationalErrorCode, OutcomeCode, TaskError};
use crate::core::task::result::{read_task_result_text, TaskResult, TaskResultText};
use crate::worker::execution::{
    persist_failed_attempt, persist_ok_result, persist_undecodable_result, phase_retry_delay,
    run_phase2, unclaim_task_with_retry, FinalizeError, FinalizeOutcome, Phase2Work,
    PHASE1_MAX_RETRIES,
};
use crate::worker::executor::protocol::{ResultEnvelopeText, RunEnd, StopReason};
use crate::worker::retry::{admit_retry, RetryAdmission};

/// The text of a worker failure with no detail (Python
/// `_WORKER_FAILURE_FALLBACK`).
const WORKER_FAILURE_FALLBACK: &str = "Worker failure";

/// One write of phase 1, sent again on a retryable database error.
enum PhaseOneWrite {
    Success { value: Vec<u8> },
    Undecodable { diagnosis: String },
    Failure(FinishFailedAttempt),
}

/// Whether a write that found the row CLAIMED releases the claim: the
/// child was killed before it set the row RUNNING.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimedRowRule {
    /// The child reported a result, so the row must be RUNNING.
    Refuse,
    /// A killed child: a CLAIMED row goes back to the queue with no attempt.
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
/// the end, the retry the policy admits for it, at `placement`. Pure.
pub fn killed_child_write(
    task: &ClaimedTask,
    worker_id: &str,
    code: BuiltInTaskCode,
    message: String,
    placement: RetryPlacement,
) -> FinishFailedAttempt {
    let code_text = code.value();
    let error = TaskError::builtin(code, message.clone());
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

/// Writes the end of each attempt with the existing statements.
pub struct OutcomeWriter {
    broker: Arc<PostgresBroker>,
    workflows: Arc<WorkflowSpecRegistry>,
    worker_id: String,
    payload_policy: PayloadPolicy,
    retention: RetentionConfig,
}

impl OutcomeWriter {
    pub fn new(
        broker: Arc<PostgresBroker>,
        workflows: Arc<WorkflowSpecRegistry>,
        worker_id: String,
        payload_policy: PayloadPolicy,
        retention: RetentionConfig,
    ) -> Self {
        Self {
            broker,
            workflows,
            worker_id,
            payload_policy,
            retention,
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
                self.phase_one(task, write, ClaimedRowRule::Refuse).await
            }
            AttemptEnd::Reported(RunEnd::WorkerFailure(detail)) => {
                let write = worker_failure_write(task, &self.worker_id, detail.as_str());
                self.phase_one(task, PhaseOneWrite::Failure(write), ClaimedRowRule::Refuse)
                    .await
            }
            AttemptEnd::TimedOut { timeout, retry } => {
                let write = killed_child_write(
                    task,
                    &self.worker_id,
                    OutcomeCode::TaskTimeout.into(),
                    format!("task exceeded timeout_ms={}", timeout.get()),
                    retry,
                );
                self.phase_one(task, PhaseOneWrite::Failure(write), ClaimedRowRule::Release)
                    .await
            }
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
                self.phase_one(task, PhaseOneWrite::Failure(write), ClaimedRowRule::Release)
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
                self.phase_one(task, PhaseOneWrite::Failure(write), ClaimedRowRule::Release)
                    .await
            }
        }
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
    ) -> Result<FinalizeOutcome, FinalizeError> {
        let workflow = task.kind.is_workflow_task();
        match write {
            PhaseOneWrite::Success { value } => {
                persist_ok_result(
                    &self.broker,
                    task.task_id,
                    value,
                    fence(task, &self.worker_id),
                    workflow,
                    &task.queue_name,
                )
                .await
            }
            PhaseOneWrite::Undecodable { diagnosis } => {
                persist_undecodable_result(
                    &self.broker,
                    task.task_id,
                    fence(task, &self.worker_id),
                    workflow,
                    diagnosis,
                )
                .await
            }
            PhaseOneWrite::Failure(command) => {
                persist_failed_attempt(&self.broker, command, workflow).await
            }
        }
    }

    /// The write and at most [`PHASE1_MAX_RETRIES`] retries of it; then
    /// phase 2 when owed.
    async fn phase_one(
        &self,
        task: &ClaimedTask,
        write: PhaseOneWrite,
        claimed_row: ClaimedRowRule,
    ) -> Settlement {
        let mut retries_done: u32 = 0;
        let outcome = loop {
            match self.write_once(task, &write).await {
                Ok(outcome) => break outcome,
                Err(error) if error.retryable && retries_done < PHASE1_MAX_RETRIES => {
                    retries_done += 1;
                    tracing::warn!(
                        task_id = %task.task_id,
                        retry = retries_done,
                        max = PHASE1_MAX_RETRIES,
                        error = %error,
                        "the outcome write failed; sending it again",
                    );
                    tokio::time::sleep(Duration::from_secs_f64(phase_retry_delay(retries_done)))
                        .await;
                }
                Err(error) => {
                    tracing::error!(
                        task_id = %task.task_id,
                        error = %error,
                        "the outcome write failed for good; the row waits for recovery",
                    );
                    return Settlement::RetriesExhausted;
                }
            }
        };
        match (outcome, claimed_row) {
            (
                FinalizeOutcome::Terminal {
                    is_success,
                    capacity_notified,
                },
                _,
            ) => {
                self.phase_two(task, is_success, capacity_notified).await;
                Settlement::Written
            }
            (FinalizeOutcome::Retried | FinalizeOutcome::Finalized, _) => Settlement::Written,
            (FinalizeOutcome::Refused, ClaimedRowRule::Refuse) => Settlement::NotHeld,
            (FinalizeOutcome::Refused, ClaimedRowRule::Release) => {
                self.release(task, "the child was killed before the task ran")
                    .await
            }
        }
    }

    async fn phase_two(&self, task: &ClaimedTask, is_success: bool, capacity_notified: bool) {
        run_phase2(
            self.broker.pool(),
            &self.workflows,
            Phase2Work {
                task_id: task.task_id,
                is_success,
                queue_name: task.queue_name.clone(),
                is_workflow_task: task.kind.is_workflow_task(),
                capacity_notified,
            },
            &self.payload_policy,
            &self.retention,
        )
        .await;
    }

    /// Release a claim that is still this worker's, at this generation: the
    /// row goes back to the queue. A row in any other state is left alone.
    async fn release(&self, task: &ClaimedTask, reason: &str) -> Settlement {
        match unclaim_task_with_retry(
            &self.broker,
            task.task_id,
            &self.worker_id,
            task.claimed_at,
            reason,
        )
        .await
        {
            Ok(true) => Settlement::Written,
            Ok(false) => Settlement::NotHeld,
            Err(error) => {
                tracing::error!(
                    task_id = %task.task_id,
                    error = %error,
                    "the claim could not be released; its lease returns the row",
                );
                Settlement::RetriesExhausted
            }
        }
    }

    /// The child stopped before RUNNING and wrote what the case needs. An
    /// expired workflow task replays phase 2 (Python `classify.py`).
    async fn after_stop(&self, task: &ClaimedTask, reason: StopReason) -> Settlement {
        match (reason, task.kind.is_workflow_task()) {
            (StopReason::TaskExpired, true) => {
                self.phase_two(task, false, false).await;
                Settlement::NothingOwed
            }
            (
                StopReason::TaskExpired
                | StopReason::ClaimLost
                | StopReason::OwnershipUnconfirmed
                | StopReason::WorkflowStopped
                | StopReason::NodeNotLinked,
                _,
            ) => Settlement::NothingOwed,
        }
    }
}

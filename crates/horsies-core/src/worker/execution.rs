//! Task execution phase decomposition.
//!
//! Mirrors Python's `_finalize_after` architecture with named phase boundaries
//! and phase-aware error recovery:
//!
//! - **Phase 0** — `confirm_ownership_and_set_running`: CLAIMED → RUNNING
//! - **Phase 1a** — `build_task_input`: the stored input, with workflow context
//! - **Phase 1b** — `execute_task`: async/blocking invocation with panic isolation
//! - **Phase 2a** — `schedule_retry_for_task`: atomic requeue + attempt record
//! - **Finalize Phase 1** — `persist_terminal_state`: terminal DB write
//! - **Finalize Phase 2** — `finalize_workflow_phase`: workflow callback + NOTIFY
//!
//! Error recovery uses per-phase retry budgets (Phase 1: 3, Phase 2: 5).
//! Phase 2 replays consume the durable v33 outbox row; terminal live rows are
//! no longer a recovery authority after the v35 move-to-history cutover.

use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::broker::attempt_writes::{
    finish_failed_attempt, finish_workflow_success, AttemptFailure, AttemptFailureKind,
    FinishFailedAttempt, FinishWorkflowSuccess, RetryPlacement, RetryRequest,
};
use crate::broker::listener::notify_task_queue;
use crate::broker::terminalization::terminalize;
use crate::broker::{ClaimedTaskRow, PostgresBroker, SetRunningRow};
use crate::core::codec::user_value::check_task_input;
use crate::core::config::payload::{enforce_payload_policy, PayloadKind, PayloadPolicy};
use crate::core::config::recovery::RecoveryConfig;
use crate::core::config::retention::RetentionConfig;
use crate::core::lifecycle::{
    AfterFailure, AttemptWriteRefusal, FailedAttemptOutcome, OwnedClaim, TerminalizationCommand,
    TerminalizationOutcome, WorkflowSuccessOutcome,
};
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::task::error::{OperationalErrorCode, OutcomeCode, TaskError};
use crate::core::task::fn_trait::RegisteredTask;
use crate::core::task::result::TaskResult;
use crate::core::types::status::TaskStatus;
use crate::core::workflow::context::{WORKFLOW_CTX_KWARG, WORKFLOW_META_KWARG};
use crate::workflow_engine::args_merge::unwrap_args_from_entries;
use crate::workflow_engine::phase2_recovery::terminal_status_of_pending_kind;

use crate::worker::retry::{
    admit_retry, read_stored_options, RetryAdmission, RunPermit, StoredOptions,
};

#[cfg(test)]
fn test_uuid(value: &str) -> Uuid {
    Uuid::parse_str(value).expect("test identity must be UUID")
}

// Re-export for worker.rs orchestrator.
pub(crate) use self::parse::parse_workflow_ctx;

// ---------------------------------------------------------------------------
// Phase types
// ---------------------------------------------------------------------------

/// Outcome of confirming task ownership and transitioning to RUNNING.
pub(crate) enum OwnershipOutcome {
    /// Task transitioned to RUNNING; carries the attempt context row
    /// (`started_at` + retry/expiry columns) read under the transition's lock.
    Running(SetRunningRow),
    /// Task expired while CLAIMED but before user code started.
    ExpiredBeforeStart,
    /// Ownership lost or workflow stopped — skip execution.
    Aborted,
}

/// Outcome of Phase 1 finalize: persisting the terminal task state.
pub(crate) enum FinalizeOutcome {
    /// Task completed or failed terminally. Needs Phase 2 (workflow callback).
    Terminal {
        is_success: bool,
        /// Phase 1 already fired the capacity wake (the fused success path
        /// notifies in-statement); Phase 2 must not wake capacity twice.
        capacity_notified: bool,
    },
    /// Task was requeued for retry. No workflow callback needed.
    Retried,
    /// Plain (non-workflow) task fully finalized in one statement (fence,
    /// attempt, move and capacity wake), or a replay of a write that owes
    /// nothing more. No Phase 2 needed.
    Finalized,
    /// The fenced write found the row not held by this attempt (another
    /// claim generation, another state, or no row) and wrote nothing. Nothing
    /// is owed; the writer logged what it saw. Python returns `Ok(None)` for
    /// these refusals (`core/worker/finalize.py`).
    Refused,
}

/// Data needed to run Phase 2 after the semaphore permit is released.
///
/// Returned by `execute_and_finalize` so the caller can drop the permit
/// before running potentially slow workflow callbacks + retries.
pub(crate) struct Phase2Work {
    pub task_id: Uuid,
    pub is_success: bool,
    pub queue_name: String,
    pub is_workflow_task: bool,
    /// Phase 1 already woke queue capacity; Phase 2 skips its notify.
    pub capacity_notified: bool,
}

/// Which finalize phase failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinalizeStage {
    /// Phase 1: persisting the terminal task state.
    Phase1Persist,
    /// Phase 2: workflow advancement + capacity notifications.
    Phase2Workflow,
}

/// Structured error for finalize phase failures.
///
/// Carries enough context for the phase-aware retry logic to decide
/// whether and how to replay.
#[derive(Debug)]
pub(crate) struct FinalizeError {
    pub stage: FinalizeStage,
    pub task_id: Uuid,
    pub message: String,
    pub retryable: bool,
}

impl std::fmt::Display for FinalizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FinalizeError(stage={:?}, task={}, retryable={}, msg={})",
            self.stage, self.task_id, self.retryable, self.message
        )
    }
}

// ---------------------------------------------------------------------------
// Finalize retry bounds (Python `core/worker/runtime.py`)
// ---------------------------------------------------------------------------

/// Phase 1 finalize gets 3 retries (matches Python `_FINALIZE_PHASE1_MAX_RETRIES`).
pub(crate) const PHASE1_MAX_RETRIES: u32 = 3;
/// Phase 2 finalize gets 5 retries (matches Python `_FINALIZE_PHASE2_MAX_RETRIES`).
const PHASE2_MAX_RETRIES: u32 = 5;
/// Base delay for phase-level retry backoff.
const PHASE_RETRY_BASE_DELAY_S: f64 = 0.5;
/// Maximum delay for phase-level retry backoff.
const PHASE_RETRY_MAX_DELAY_S: f64 = 15.0;

// ---------------------------------------------------------------------------
// Phase 0: Ownership confirmation (CLAIMED → RUNNING)
// ---------------------------------------------------------------------------

/// Confirm task ownership and transition CLAIMED → RUNNING.
///
/// Also updates the corresponding workflow_task status if applicable.
/// On failure, attempts to unclaim the task or mark it as skipped.
///
/// `claimed_at` is the claim generation this dispatch was born from; it
/// fences `set_running`, the pre-start expiry, and the failure-path unclaim
/// so none of them can act on a row the same worker re-claimed (C10).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn confirm_ownership_and_set_running(
    broker: &PostgresBroker,
    task_id: Uuid,
    worker_id: &str,
    pid: i32,
    hostname: &str,
    is_workflow_task: bool,
    claimed_at: chrono::DateTime<Utc>,
    orphan_self_heal: bool,
) -> OwnershipOutcome {
    // Workflow tasks: the node RUNNING handoff precedes the task transition,
    // while the row is still CLAIMED. The match set is idempotent across
    // crash-replays (a node already RUNNING matches again). The first handoff
    // stamps `started_at`; a RUNNING replay preserves the original timestamp.
    // Zero matched rows
    // is the free orphan signal: no workflow_task linkage in a runnable state
    // means this row can never legitimately progress — with self-heal on, the
    // still-CLAIMED row is cancelled through `horsies_cancel_owned_orphan`,
    // which re-verifies the linkage under its own lock and refuses (leaving
    // the task to run) when a runnable link exists after all. A transient
    // handoff failure stays non-fatal: the update is informational
    // (COMPLETE_WORKFLOW_TASK_SQL accepts an ENQUEUED workflow_task), so the
    // task proceeds. Gated on `is_workflow_task`: a plain task pays no round
    // trip here (P1).
    if is_workflow_task {
        match sqlx::query_scalar::<_, bool>(
            "WITH linked AS MATERIALIZED (
                SELECT id, status FROM horsies_workflow_tasks
                WHERE task_id = $1 AND status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
                FOR UPDATE
             ), changed AS (
                UPDATE horsies_workflow_tasks n SET status = 'RUNNING', started_at = NOW()
                FROM linked l WHERE n.id = l.id AND l.status <> 'RUNNING' RETURNING n.id
             ) SELECT EXISTS(SELECT 1 FROM linked)",
        )
        .bind(task_id)
        .fetch_one(broker.pool())
        .await
        {
            Ok(false) if orphan_self_heal => {
                match terminalize(
                    broker.pool(),
                    &TerminalizationCommand::CancelOwnedOrphan {
                        task_id,
                        fence: OwnedClaim {
                            worker_id: worker_id.to_owned(),
                            claimed_at,
                        },
                    },
                )
                .await
                {
                    Ok(outcomes)
                        if matches!(
                            outcomes.first(),
                            Some(TerminalizationOutcome::Applied { .. })
                        ) =>
                    {
                        tracing::warn!(
                            task_id = %task_id,
                            "workflow task orphaned (no runnable workflow_task \
                             linkage); cancelled before start",
                        );
                        return OwnershipOutcome::Aborted;
                    }
                    Ok(_) => {
                        // Refused: a runnable link exists after all (e.g. a
                        // crash-replay whose node moved between statements),
                        // or ownership moved — set_running below re-judges.
                    }
                    Err(e) => {
                        tracing::warn!(
                            task_id = %task_id,
                            error = %e,
                            "orphan check failed; proceeding to set_running",
                        );
                    }
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    task_id = %task_id,
                    error = %e,
                    "failed to update workflow task to RUNNING; proceeding with \
                     execution (workflow_task left as-is, which completion \
                     tolerates)",
                );
            }
        }
    }

    match broker
        .set_running(task_id, worker_id, pid, hostname, "worker", claimed_at)
        .await
    {
        Ok(Some(running)) => OwnershipOutcome::Running(running),
        Ok(None) => {
            match broker
                .expire_claimed_task_before_start(task_id, worker_id)
                .await
            {
                Ok(Some(result_json)) => {
                    tracing::info!(
                        task_id = %task_id,
                        "task expired before execution started",
                    );
                    let _ = result_json;
                    return OwnershipOutcome::ExpiredBeforeStart;
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(
                        task_id = %task_id,
                        error = %e,
                        "failed to check claimed task expiry before start",
                    );
                }
            }

            match broker.get_workflow_status_for_task(task_id).await {
                Ok(Some(ref wf_status)) if wf_status == "PAUSED" || wf_status == "CANCELLED" => {
                    tracing::info!(
                        task_id = %task_id,
                        workflow_status = %wf_status,
                        "skipping task - workflow is stopped",
                    );
                    if let Err(e) = handle_workflow_stop_with_retry(
                        broker, task_id, wf_status, worker_id, claimed_at,
                    )
                    .await
                    {
                        tracing::error!(
                            task_id = %task_id,
                            error = %e,
                            workflow_status = %wf_status,
                            "failed to handle workflow stop before task start",
                        );
                    }
                }
                _ => {
                    tracing::debug!(task_id = %task_id, "ownership lost, skipping execution");
                }
            }
            OwnershipOutcome::Aborted
        }
        Err(e) => {
            tracing::error!(task_id = %task_id, error = %e, "failed to set RUNNING, requeueing");
            if let Err(ue) = unclaim_task_with_retry(
                broker,
                task_id,
                worker_id,
                claimed_at,
                "set RUNNING failed",
            )
            .await
            {
                tracing::error!(task_id = %task_id, error = %ue, "failed to unclaim task after RUNNING transition error");
            }
            OwnershipOutcome::Aborted
        }
    }
}

const PRESTART_DB_RETRY_ATTEMPTS: u32 = 3;

async fn handle_workflow_stop_with_retry(
    broker: &PostgresBroker,
    task_id: Uuid,
    workflow_status: &str,
    worker_id: &str,
    claimed_at: chrono::DateTime<Utc>,
) -> Result<(), crate::broker::BrokerError> {
    let mut last_err: Option<crate::broker::BrokerError> = None;

    for attempt in 1..=PRESTART_DB_RETRY_ATTEMPTS {
        match broker
            .handle_workflow_stop_before_start(task_id, workflow_status, worker_id, claimed_at)
            .await
        {
            Ok(()) => return Ok(()),
            Err(err) => {
                let retryable = err.is_retryable() && attempt < PRESTART_DB_RETRY_ATTEMPTS;
                tracing::warn!(
                    task_id = %task_id,
                    workflow_status = %workflow_status,
                    attempt,
                    max = PRESTART_DB_RETRY_ATTEMPTS,
                    retryable,
                    error = %err,
                    "failed to handle workflow stop before task start",
                );
                last_err = Some(err);
                if !retryable {
                    break;
                }
                tokio::time::sleep(Duration::from_secs_f64(phase_retry_delay(attempt))).await;
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        crate::broker::BrokerError::ConnectionFailed(
            "workflow stop handling failed without a captured broker error".to_owned(),
        )
    }))
}

pub(crate) async fn unclaim_task_with_retry(
    broker: &PostgresBroker,
    task_id: Uuid,
    worker_id: &str,
    claimed_at: chrono::DateTime<Utc>,
    context: &str,
) -> Result<bool, crate::broker::BrokerError> {
    let mut last_err: Option<crate::broker::BrokerError> = None;

    for attempt in 1..=PRESTART_DB_RETRY_ATTEMPTS {
        match broker.unclaim_task(task_id, worker_id, claimed_at).await {
            Ok(applied) => {
                if !applied {
                    tracing::warn!(
                        task_id = %task_id,
                        worker_id = %worker_id,
                        context,
                        "unclaim skipped: task no longer CLAIMED by worker",
                    );
                }
                return Ok(applied);
            }
            Err(err) => {
                let retryable = err.is_retryable() && attempt < PRESTART_DB_RETRY_ATTEMPTS;
                tracing::warn!(
                    task_id = %task_id,
                    worker_id = %worker_id,
                    context,
                    attempt,
                    max = PRESTART_DB_RETRY_ATTEMPTS,
                    retryable,
                    error = %err,
                    "failed to unclaim task",
                );
                last_err = Some(err);
                if !retryable {
                    break;
                }
                tokio::time::sleep(Duration::from_secs_f64(phase_retry_delay(attempt))).await;
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        crate::broker::BrokerError::ConnectionFailed(
            "unclaim failed without a captured broker error".to_owned(),
        )
    }))
}

// ---------------------------------------------------------------------------
// Phase 1a: Build the task input from the one stored input value
// ---------------------------------------------------------------------------

/// The bytes that the task decodes with its declared input type: the claimed
/// row's stored input, with the workflow context member renamed when the task
/// takes it.
pub(crate) fn build_claimed_task_input(
    row: &ClaimedTaskRow,
    accepts_workflow_ctx: bool,
) -> Result<Vec<u8>, TaskError> {
    build_task_input(&row.input, accepts_workflow_ctx)
}

/// Build the task input bytes from one encoded input value.
///
/// Shared by [`build_claimed_task_input`] (execution) and `app.check()`'s input
/// validation (schedules and workflow nodes), so check-time dry-runs read the
/// input exactly as execution does.
///
/// The input must pass the core's strict JSON read rule; a refused text fails
/// the task before user code runs. An object input is Python's keyword input;
/// its engine members (Python `core/worker/child_runner.py:1546-1645`):
/// - `WORKFLOW_CTX_KWARG` becomes `workflow_ctx` when `accepts_workflow_ctx`,
///   and is removed otherwise (check-time callers pass `false`: no runtime
///   context);
/// - `WORKFLOW_META_KWARG` is removed;
/// - an `args_from` entry is replaced by its `inner` result envelope, which
///   the receiving `TaskResult` field decodes with its own type.
///
/// Every other member value, and any other input except `null` as one value,
/// follows the rules of a user value (no engine key, at most
/// `MAX_VALUE_JSON_DEPTH` levels), and no other member name is an engine key.
/// A refusal is `WORKER_SERIALIZATION_ERROR`, as Python's
/// `_serialization_error_response`.
///
/// An input with no engine member passes byte for byte. An input with one is
/// re-encoded, so an integer that the parse would round is refused instead of
/// changed.
pub(crate) fn build_task_input(
    input: &str,
    accepts_workflow_ctx: bool,
) -> Result<Vec<u8>, TaskError> {
    let value: serde_json::Value = crate::core::codec::from_json_str(input)
        .map_err(|e| serialization_error(format!("stored input refused: {e}")))?;
    let no_engine_members: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut object = match value {
        serde_json::Value::Object(object) => object,
        serde_json::Value::Null => return Ok(input.as_bytes().to_vec()),
        other => {
            check_task_input(
                std::slice::from_ref(&other),
                &serde_json::Map::new(),
                &no_engine_members,
            )
            .map_err(|refusal| {
                serialization_error(format!("failed to decode task input: {refusal}"))
            })?;
            return Ok(input.as_bytes().to_vec());
        }
    };

    let workflow_ctx = object.remove(WORKFLOW_CTX_KWARG);
    let workflow_meta = object.remove(WORKFLOW_META_KWARG);
    let args_from_members = unwrap_args_from_entries(&mut object)
        .map_err(|refusal| serialization_error(refusal.to_string()))?;
    check_task_input(&[], &object, &args_from_members).map_err(|refusal| {
        serialization_error(format!("failed to decode task input: {refusal}"))
    })?;
    if workflow_ctx.is_none() && workflow_meta.is_none() && args_from_members.is_empty() {
        return Ok(input.as_bytes().to_vec());
    }
    crate::core::history::rerun::input_envelope::validate_integer_domain(input).map_err(
        |detail| {
            serialization_error(format!(
                "stored input holds a number that a rewrite would round: {detail}"
            ))
        },
    )?;

    if let Some(ctx_value) = workflow_ctx {
        if accepts_workflow_ctx {
            let workflow_ctx = parse_workflow_ctx(ctx_value)?;
            let ctx_json = serde_json::to_value(&workflow_ctx).map_err(|e| {
                serialization_error(format!("failed to serialize workflow context: {}", e))
            })?;
            object.insert("workflow_ctx".to_owned(), ctx_json);
        }
    }

    serde_json::to_vec(&serde_json::Value::Object(object))
        .map_err(|e| serialization_error(format!("failed to serialize task input: {}", e)))
}

fn serialization_error(message: String) -> TaskError {
    TaskError::builtin(OperationalErrorCode::WorkerSerializationError, message)
}

// ---------------------------------------------------------------------------
// Phase 1b: Task execution (with panic isolation)
// ---------------------------------------------------------------------------

/// Execute a registered task function with panic isolation.
///
/// Async tasks are spawned into a new tokio task for cancellation safety.
/// Blocking tasks use `spawn_blocking` with `catch_unwind` for panic safety.
///
/// `timeout` is the per-task `timeout_ms` deadline (if any), measured around
/// user-code execution. On expiry the task resolves to `OutcomeCode::TaskTimeout`
/// and the normal finalize path decides fail-vs-retry (via `auto_retry_for`).
/// Async tasks are aborted on timeout; blocking threads cannot be aborted and
/// run to completion with their result discarded (see `TaskOptions::timeout_ms`).
pub(crate) async fn execute_task(
    task_fn: RegisteredTask,
    task_input: Vec<u8>,
    timeout: Option<Duration>,
) -> TaskResult<Vec<u8>> {
    match task_fn {
        RegisteredTask::Async { task: f, .. } => {
            let mut handle = tokio::spawn(async move { f.execute(&task_input).await });
            let join = match timeout {
                Some(dur) => {
                    tokio::select! {
                        joined = &mut handle => joined,
                        _ = tokio::time::sleep(dur) => {
                            // Cancel the running future at its next await point;
                            // unlike a blocking thread, an async task is abortable.
                            handle.abort();
                            return task_timeout_error(dur);
                        }
                    }
                }
                None => handle.await,
            };
            match join {
                Ok(r) => r,
                Err(join_err) if join_err.is_cancelled() => {
                    task_timeout_error(timeout.unwrap_or_default())
                }
                Err(join_err) => TaskResult::Err(TaskError::builtin(
                    OperationalErrorCode::UnhandledException,
                    format!("async task panicked: {}", join_err),
                )),
            }
        }
        RegisteredTask::Blocking { task: f, .. } => {
            let handle = tokio::task::spawn_blocking(move || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f.execute(&task_input)))
            });
            let join = match timeout {
                Some(dur) => {
                    tokio::select! {
                        joined = handle => joined,
                        _ = tokio::time::sleep(dur) => {
                            // A blocking thread cannot be aborted in-process: it
                            // runs to completion and its result is discarded. We
                            // only stop awaiting it and finalize as TASK_TIMEOUT.
                            return task_timeout_error(dur);
                        }
                    }
                }
                None => handle.await,
            };
            match join {
                Ok(Ok(r)) => r,
                Ok(Err(_panic)) => TaskResult::Err(TaskError::builtin(
                    OperationalErrorCode::UnhandledException,
                    "blocking task panicked".to_owned(),
                )),
                Err(join_err) => TaskResult::Err(TaskError::builtin(
                    OperationalErrorCode::UnhandledException,
                    format!("blocking task join error: {}", join_err),
                )),
            }
        }
    }
}

/// Build the `TASK_TIMEOUT` error for a task that exceeded its `timeout_ms`.
fn task_timeout_error(timeout: Duration) -> TaskResult<Vec<u8>> {
    TaskResult::Err(TaskError::builtin(
        OutcomeCode::TaskTimeout,
        format!("task exceeded timeout_ms={}", timeout.as_millis()),
    ))
}

// ---------------------------------------------------------------------------
// Finalize Phase 1: Persist the outcome of the attempt
// ---------------------------------------------------------------------------

/// Phase 1 of finalization: write the outcome of the attempt in one statement.
///
/// - The success of a plain task: `horsies_complete_task_fused`.
/// - The success of a workflow task: `horsies_finish_workflow_success`.
/// - A failure, and a result that is not valid JSON:
///   `horsies_finish_failed_attempt`, with the retry that the policy admits.
///
/// Each statement runs alone on the pool and decides the whole outcome under
/// its own locks (the task advisory lock, then the row), and writes the
/// attempt row from the locked row on the database clock. A database error
/// never decides between a retry and a failure: it is a phase-1 error,
/// retryable when the connection failed, and the next try sends the same
/// statement (Python `core/worker/finalize.py`, `_persist_failed_attempt`).
/// A refusal is an outcome ([`FinalizeOutcome::Refused`]), not an error.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn persist_terminal_state(
    broker: &PostgresBroker,
    task_id: Uuid,
    result: TaskResult<Vec<u8>>,
    row: &ClaimedTaskRow,
    stored_options: &StoredOptions,
    running: &SetRunningRow,
    worker_id: &str,
    payload_policy: &PayloadPolicy,
) -> Result<FinalizeOutcome, FinalizeError> {
    let fence = OwnedClaim {
        worker_id: worker_id.to_owned(),
        claimed_at: row.claimed_at,
    };
    match result {
        TaskResult::Ok(ref result_bytes) => {
            // Warn-only: results are never rejected over size (the work is
            // done; destroying it would convert a size concern into data
            // loss). Parity with horsies PR #208.
            enforce_payload_policy(
                payload_policy,
                &row.task_name,
                PayloadKind::Result,
                result_bytes.len(),
            );
            persist_ok_result(
                broker,
                task_id,
                result_bytes,
                fence,
                row.is_workflow_task,
                &row.queue_name,
            )
            .await
        }
        TaskResult::Err(ref task_error) => {
            // The stored error keeps its code when its `data` cannot be stored:
            // the serializer stores `{"data_refused": <cause>}` in its place
            // (Python `reportable_error`, `core/codec/typed.py:885-934`).
            if let Some(refusal) = task_error.storage_refusal() {
                tracing::error!(
                    task_id = %task_id,
                    error_code = %task_error.error_code,
                    cause = %refusal,
                    "data of a TaskError cannot be stored; it is stored as data_refused",
                );
            }
            let error_code = Some(task_error.error_code.to_string());
            let retry = retry_request_for(running, stored_options, error_code.as_deref());
            let result_json =
                serde_json::to_string(&TaskResult::<serde_json::Value>::Err(task_error.clone()))
                    .map_err(|error| envelope_error(task_id, error))?;
            // Warn-only: error envelopes can carry oversized data too.
            enforce_payload_policy(
                payload_policy,
                &row.task_name,
                PayloadKind::Result,
                result_json.len(),
            );
            let command = FinishFailedAttempt {
                task_id,
                fence,
                retry,
                result_json,
                error_code,
                failed_reason: None,
                attempt: AttemptFailure {
                    kind: AttemptFailureKind::Failed,
                    error_message: task_error.message.clone(),
                    failed_reason: None,
                },
            };
            persist_failed_attempt(broker, &command, row.is_workflow_task).await
        }
    }
}

/// The retry that the task's policy admits for this failure, with its delay
/// (Python `retry_request_for`). Pure. `retry_count` and `max_retries` come
/// from the start of this claim generation; the write fences on that count.
/// The deadline is judged by the write, on the database clock.
fn retry_request_for(
    running: &SetRunningRow,
    stored_options: &StoredOptions,
    error_code: Option<&str>,
) -> RetryRequest {
    match admit_retry(
        running.retry_count,
        running.max_retries,
        stored_options.retry(),
        error_code,
    ) {
        RetryAdmission::Admitted(rule) => RetryRequest::RetryAfter {
            expected_retry_count: running.retry_count,
            delay_seconds: rule.delay_seconds((running.retry_count + 1).unsigned_abs()),
            placement: RetryPlacement::SameClass,
        },
        RetryAdmission::RetriesSpent | RetryAdmission::NoPolicy | RetryAdmission::CodeNotListed => {
            RetryRequest::NoRetry
        }
    }
}

/// Persist a success, or fail a result that is not valid JSON.
pub(crate) async fn persist_ok_result(
    broker: &PostgresBroker,
    task_id: Uuid,
    result_bytes: &[u8],
    fence: OwnedClaim,
    is_workflow_task: bool,
    queue_name: &str,
) -> Result<FinalizeOutcome, FinalizeError> {
    // Wrap the task's result bytes via `&RawValue`: parsing validates the JSON,
    // and the adjacently-tagged wrapper embeds the original bytes verbatim
    // instead of building and re-serializing a full `Value` tree (P6).
    let raw = match serde_json::from_slice::<&serde_json::value::RawValue>(result_bytes) {
        Ok(raw) => raw,
        Err(parse_error) => {
            return persist_undecodable_result(
                broker,
                task_id,
                fence,
                is_workflow_task,
                &parse_error.to_string(),
            )
            .await
        }
    };
    let result_json = serde_json::to_string(&TaskResult::Ok(raw))
        .map_err(|error| envelope_error(task_id, error))?;
    match is_workflow_task {
        true => persist_workflow_success(broker, task_id, fence, result_json).await,
        false => persist_plain_success(broker, task_id, fence, result_json, queue_name).await,
    }
}

/// The success of a plain task: one statement locks the RUNNING row under the
/// full fence, writes the attempt, moves the row and sends the capacity wake.
async fn persist_plain_success(
    broker: &PostgresBroker,
    task_id: Uuid,
    fence: OwnedClaim,
    result_json: String,
    queue_name: &str,
) -> Result<FinalizeOutcome, FinalizeError> {
    let command = TerminalizationCommand::CompleteTaskFused {
        task_id,
        fence,
        result_json,
        notify_channel: format!("task_queue_{queue_name}"),
        notify_payload: format!("capacity:{task_id}"),
    };
    let outcomes = terminalize(broker.pool(), &command)
        .await
        .map_err(|error| phase1_error(task_id, "complete-fused", error))?;
    match outcomes.into_iter().next() {
        // A replay found its own class committed: nothing is owed.
        Some(
            TerminalizationOutcome::Applied { .. } | TerminalizationOutcome::AlreadyApplied { .. },
        ) => Ok(FinalizeOutcome::Finalized),
        Some(
            TerminalizationOutcome::LostClaim { .. }
            | TerminalizationOutcome::SourceStateConflict { .. }
            | TerminalizationOutcome::TaskAbsent { .. },
        ) => Ok(FinalizeOutcome::Refused),
        None => Err(FinalizeError {
            stage: FinalizeStage::Phase1Persist,
            task_id,
            message: "complete-fused returned no outcome".to_owned(),
            retryable: false,
        }),
    }
}

/// The success of a workflow task, in one statement (Python
/// `core/worker/workflow_success.py`). Phase 2 is owed after it applies.
async fn persist_workflow_success(
    broker: &PostgresBroker,
    task_id: Uuid,
    fence: OwnedClaim,
    result_json: String,
) -> Result<FinalizeOutcome, FinalizeError> {
    let command = FinishWorkflowSuccess {
        task_id,
        fence,
        result_json,
    };
    let outcome = finish_workflow_success(broker.pool(), &command)
        .await
        .map_err(|error| phase1_error(task_id, "workflow success", error))?;
    match outcome {
        WorkflowSuccessOutcome::SuccessApplied { .. } => Ok(FinalizeOutcome::Terminal {
            is_success: true,
            capacity_notified: false,
        }),
        WorkflowSuccessOutcome::Refused(AttemptWriteRefusal::AlreadyApplied { .. }) => {
            replay_of_committed_write(broker.pool(), task_id, true).await
        }
        WorkflowSuccessOutcome::Refused(
            AttemptWriteRefusal::LostClaim { .. }
            | AttemptWriteRefusal::SourceStateConflict { .. }
            | AttemptWriteRefusal::TaskAbsent { .. },
        ) => Ok(FinalizeOutcome::Refused),
    }
}

/// A result that is not valid JSON fails the task with
/// `WORKER_SERIALIZATION_ERROR`, never retried, with the decode diagnosis as
/// the attempt message (Python `ResultUndecodable`).
pub(crate) async fn persist_undecodable_result(
    broker: &PostgresBroker,
    task_id: Uuid,
    fence: OwnedClaim,
    is_workflow_task: bool,
    diagnosis: &str,
) -> Result<FinalizeOutcome, FinalizeError> {
    let message = format!("failed to parse task result JSON: {diagnosis}");
    let error = TaskError::builtin(
        OperationalErrorCode::WorkerSerializationError,
        message.clone(),
    );
    let result_json = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(error))
        .map_err(|error| envelope_error(task_id, error))?;
    let command = FinishFailedAttempt {
        task_id,
        fence,
        retry: RetryRequest::NoRetry,
        result_json,
        error_code: Some(OperationalErrorCode::WorkerSerializationError.to_string()),
        failed_reason: None,
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some(message),
            failed_reason: None,
        },
    };
    persist_failed_attempt(broker, &command, is_workflow_task).await
}

/// Write a failed attempt in one statement and map its outcome to what is
/// owed (Python `_after_failed_attempt`): a plain task's failure sent its
/// wake in the statement; a workflow task's failure owes phase 2; a retry
/// wakes the queue when it is due; a replay owes what the committed write
/// owed; a refusal owes nothing.
pub(crate) async fn persist_failed_attempt(
    broker: &PostgresBroker,
    command: &FinishFailedAttempt,
    is_workflow_task: bool,
) -> Result<FinalizeOutcome, FinalizeError> {
    let outcome = finish_failed_attempt(broker.pool(), command)
        .await
        .map_err(|error| phase1_error(command.task_id, "failed attempt", error))?;
    match outcome {
        FailedAttemptOutcome::FailureApplied {
            after: AfterFailure::CapacityWoken,
            ..
        } => Ok(FinalizeOutcome::Finalized),
        FailedAttemptOutcome::FailureApplied {
            after: AfterFailure::PhaseTwoOwed(_),
            ..
        } => Ok(FinalizeOutcome::Terminal {
            is_success: false,
            capacity_notified: false,
        }),
        FailedAttemptOutcome::RetryScheduled {
            retry_count,
            next_retry_at,
            queue_name,
            ..
        } => {
            wake_when_retry_due(broker.pool(), command, queue_name);
            tracing::info!(
                task_id = %command.task_id,
                retry_count,
                %next_retry_at,
                "task scheduled for retry",
            );
            Ok(FinalizeOutcome::Retried)
        }
        FailedAttemptOutcome::Refused(AttemptWriteRefusal::AlreadyApplied { .. }) => {
            replay_of_committed_write(broker.pool(), command.task_id, is_workflow_task).await
        }
        FailedAttemptOutcome::Refused(
            AttemptWriteRefusal::LostClaim { .. }
            | AttemptWriteRefusal::SourceStateConflict { .. }
            | AttemptWriteRefusal::TaskAbsent { .. },
        ) => Ok(FinalizeOutcome::Refused),
    }
}

/// Wake the queue of a scheduled retry after the delay of its request
/// (Python `RetryMixin._wake_when_retry_due`). A failed wake is logged: the
/// retried row is PENDING and the worker's poll finds it.
fn wake_when_retry_due(pool: &sqlx::PgPool, command: &FinishFailedAttempt, queue_name: String) {
    let delay_seconds = match command.retry {
        RetryRequest::RetryAfter { delay_seconds, .. } => delay_seconds,
        RetryRequest::NoRetry => unreachable!(
            "task {} was scheduled for a retry that was not requested",
            command.task_id
        ),
    };
    let pool = pool.clone();
    let task_id = command.task_id;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs_f64(delay_seconds)).await;
        if let Err(error) = notify_task_queue(&pool, &queue_name, task_id).await {
            tracing::warn!(
                task_id = %task_id,
                queue = %queue_name,
                error = %error,
                "delayed retry NOTIFY failed; worker polling fallback will recover",
            );
        }
    });
}

/// A replay of a committed write (`AlreadyApplied`). A workflow task owes
/// phase 2 while its durable phase-2 record exists, with the outcome of the
/// status that the record's kind stands for; a plain task owes nothing.
async fn replay_of_committed_write(
    pool: &sqlx::PgPool,
    task_id: Uuid,
    is_workflow_task: bool,
) -> Result<FinalizeOutcome, FinalizeError> {
    match is_workflow_task {
        true => match load_pending_phase_two(pool, task_id).await? {
            PendingPhaseTwo::Owed(status) => Ok(FinalizeOutcome::Terminal {
                is_success: status == TaskStatus::Completed,
                capacity_notified: false,
            }),
            PendingPhaseTwo::Consumed => Ok(FinalizeOutcome::Finalized),
        },
        false => Ok(FinalizeOutcome::Finalized),
    }
}

/// The phase-1 error of a failed write: retryable when the connection failed.
fn phase1_error(task_id: Uuid, write: &str, error: crate::broker::BrokerError) -> FinalizeError {
    FinalizeError {
        stage: FinalizeStage::Phase1Persist,
        task_id,
        message: format!("{write} write failed: {error}"),
        retryable: error.is_retryable(),
    }
}

/// A result envelope that does not encode is a defect of the value, not of
/// the database: not retryable.
fn envelope_error(task_id: Uuid, error: serde_json::Error) -> FinalizeError {
    FinalizeError {
        stage: FinalizeStage::Phase1Persist,
        task_id,
        message: format!("failed to encode the result envelope: {error}"),
        retryable: false,
    }
}

// ---------------------------------------------------------------------------
// Finalize Phase 2: Workflow advancement + capacity notifications
// ---------------------------------------------------------------------------

/// Classify whether a `WorkflowError` is transient (retryable).
///
/// Mirrors Python's `is_retryable_connection_error(exc)` — only DB/broker
/// connection errors are considered transient. Logic errors (serialization,
/// not-found, invalid status) are permanent.
fn is_retryable_workflow_error(e: &crate::workflow_engine::WorkflowError) -> bool {
    match e {
        crate::workflow_engine::WorkflowError::Database(_) => true,
        crate::workflow_engine::WorkflowError::Broker(be) => be.is_retryable(),
        crate::workflow_engine::WorkflowError::Serialization(_)
        | crate::workflow_engine::WorkflowError::WorkflowNotFound { .. }
        | crate::workflow_engine::WorkflowError::WorkflowTimeout { .. }
        | crate::workflow_engine::WorkflowError::WorkflowError(_)
        | crate::workflow_engine::WorkflowError::InvalidStatus(_)
        | crate::workflow_engine::WorkflowError::Validation(_) => false,
    }
}

/// Phase 2 of finalization: workflow advancement and worker wake notifications.
///
/// Separate transaction from Phase 1. If this fails after Phase 1 succeeded,
/// the terminal task result is already durable and workflow recovery can resume.
pub(crate) async fn finalize_workflow_phase(
    pool: &sqlx::PgPool,
    workflow_registry: &WorkflowSpecRegistry,
    task_id: Uuid,
    is_success: bool,
    queue_name: &str,
    is_workflow_task: bool,
    capacity_notified: bool,
    payload_policy: &PayloadPolicy,
    retention: &RetentionConfig,
) -> Result<(), FinalizeError> {
    // Workflow membership is carried on the task row (is_workflow_task), set at
    // insert time — no per-task JOIN to horsies_workflow_tasks needed here.
    if is_workflow_task {
        crate::workflow_engine::phase2_recovery::finalize_phase2(
            pool,
            task_id,
            if is_success { "COMPLETED" } else { "FAILED" },
            workflow_registry,
            payload_policy,
            retention,
        )
        .await
        .map_err(|e| {
            let retryable = is_retryable_workflow_error(&e);
            FinalizeError {
                stage: FinalizeStage::Phase2Workflow,
                task_id,
                message: format!("workflow callback failed: {}", e),
                retryable,
            }
        })?;
    }

    // Capacity notification — non-fatal if it fails. Skipped when Phase 1's
    // fused statement already fired the wake in the same commit as the
    // transition (waking twice would be harmless but wasteful).
    if !capacity_notified {
        notify_worker_capacity(pool, queue_name, task_id).await;
    }

    Ok(())
}

/// The kind of a task's phase-2 record. The record has no status column: the
/// status follows from the kind (`terminal_status_of_pending_kind`).
const LOAD_PENDING_KIND_SQL: &str =
    "SELECT terminalization_kind FROM horsies_workflow_phase2_pending WHERE task_id = $1";

/// What the phase-2 record of a committed workflow write owes, as a replay
/// reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingPhaseTwo {
    /// The record exists: phase 2 is owed for a task that ended with this
    /// status (COMPLETED, FAILED or EXPIRED).
    Owed(TaskStatus),
    /// No record: phase 2 consumed it, so the replay owes nothing.
    Consumed,
}

/// Read the phase-2 record of a workflow task after a phase-1 replay.
///
/// One statement; writes nothing. The moved history row is not read: the
/// record is the evidence that phase 2 is still owed.
///
/// Errors:
/// - the read fails: retryable;
/// - the record names a kind that writes no phase-2 record: not retryable.
async fn load_pending_phase_two(
    pool: &sqlx::PgPool,
    task_id: Uuid,
) -> Result<PendingPhaseTwo, FinalizeError> {
    let kind: Option<String> = sqlx::query_scalar(LOAD_PENDING_KIND_SQL)
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .map_err(|error| FinalizeError {
            stage: FinalizeStage::Phase2Workflow,
            task_id,
            message: format!("failed to load the phase-2 record: {error}"),
            retryable: true,
        })?;
    match kind {
        None => Ok(PendingPhaseTwo::Consumed),
        Some(kind) => terminal_status_of_pending_kind(&kind)
            .map(PendingPhaseTwo::Owed)
            .map_err(|refusal| FinalizeError {
                stage: FinalizeStage::Phase2Workflow,
                task_id,
                message: refusal.to_string(),
                retryable: false,
            }),
    }
}

#[cfg(test)]
mod replay_read_tests;

pub(crate) async fn notify_worker_capacity(pool: &sqlx::PgPool, queue_name: &str, task_id: Uuid) {
    // Wake workers of this queue to re-check capacity/backlog. The capacity
    // signal is sent only on the per-queue channel; workers no longer listen on
    // task_new. Parity with horsies PR #101 cafc9200.
    let payload = format!("capacity:{}", task_id);
    let channel = format!("task_queue_{}", queue_name);
    if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
        .bind(&channel)
        .bind(&payload)
        .execute(pool)
        .await
    {
        tracing::warn!(
            task_id = %task_id,
            channel = %channel,
            error = %e,
            "queue-specific capacity NOTIFY failed; worker polling fallback will recover",
        );
    }
}

pub(crate) async fn finalize_pre_execution_failure(
    broker: Arc<PostgresBroker>,
    row: ClaimedTaskRow,
    worker_id: String,
    hostname: String,
    task_error: TaskError,
    payload_policy: PayloadPolicy,
    orphan_self_heal: bool,
) -> Option<Phase2Work> {
    let task_id = row.id;
    let queue_name = row.queue_name.clone();
    let is_workflow_task = row.is_workflow_task;
    let pid = std::process::id() as i32;

    let running = match confirm_ownership_and_set_running(
        &broker,
        task_id,
        &worker_id,
        pid,
        &hostname,
        is_workflow_task,
        row.claimed_at,
        orphan_self_heal,
    )
    .await
    {
        OwnershipOutcome::Running(running) => running,
        OwnershipOutcome::ExpiredBeforeStart => {
            return Some(Phase2Work {
                task_id,
                is_success: false,
                queue_name,
                is_workflow_task,
                capacity_notified: false,
            });
        }
        OwnershipOutcome::Aborted => return None,
    };

    let stored_options = read_stored_options(row.task_options.as_deref());
    match retry_phase1(
        &broker,
        task_id,
        TaskResult::Err(task_error),
        &row,
        &stored_options,
        &running,
        &worker_id,
        &payload_policy,
    )
    .await
    {
        Some(FinalizeOutcome::Terminal {
            is_success,
            capacity_notified,
        }) => Some(Phase2Work {
            task_id,
            is_success,
            queue_name,
            is_workflow_task,
            capacity_notified,
        }),
        Some(FinalizeOutcome::Retried | FinalizeOutcome::Finalized | FinalizeOutcome::Refused)
        | None => None,
    }
}

// ---------------------------------------------------------------------------
// Orchestrator: execute_and_finalize (mirrors Python's _finalize_after)
// ---------------------------------------------------------------------------

/// What the finalizing marker found on the task row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinalizingMark {
    /// The row is RUNNING under this worker's claim generation; the marker is
    /// written.
    Marked,
    /// The row is absent, not RUNNING, or held by another worker or another
    /// claim generation; nothing is written.
    NotHeld,
}

/// The finalizing marker, as Python's `MARK_FINALIZING_SQL`
/// (`core/worker/child_runner.py`). The row is read by id alone in a
/// MATERIALIZED CTE that locks it, so only the primary key serves the read;
/// the fence (RUNNING, this worker, this claim generation) is one boolean
/// checked on the locked row. The generation comparison has no NULL arm.
const MARK_FINALIZING_SQL: &str = "\
WITH target AS MATERIALIZED (
    SELECT id,
           (
               status = 'RUNNING'
               AND claimed_by_worker_id = $2
               AND claimed_at = $3
           ) IS TRUE AS fenced
    FROM horsies_tasks
    WHERE id = $1
    FOR NO KEY UPDATE
)
UPDATE horsies_tasks t
SET finalizing_at = NOW(),
    finalizing_by_worker_id = $2,
    updated_at = NOW()
FROM target
WHERE t.id = target.id
  AND target.fenced";

/// Stamp the finalization handoff only for the current owner and claim
/// generation. One statement in autocommit: its row lock ends with it.
pub(crate) async fn mark_task_finalizing(
    pool: &sqlx::PgPool,
    task_id: Uuid,
    worker_id: &str,
    claimed_at: chrono::DateTime<Utc>,
) -> Result<FinalizingMark, sqlx::Error> {
    let result = sqlx::query(MARK_FINALIZING_SQL)
        .bind(task_id)
        .bind(worker_id)
        .bind(claimed_at)
        .execute(pool)
        .await?;
    match result.rows_affected() {
        0 => Ok(FinalizingMark::NotHeld),
        1 => Ok(FinalizingMark::Marked),
        changed => unreachable!(
            "finalizing marker of task {task_id} changed {changed} rows; \
             the statement reads one row by its primary key"
        ),
    }
}

/// Execute a task through Phase 1 finalize, returning Phase 2 work if needed.
///
/// Orchestrates:
/// 1. Confirm ownership (CLAIMED → RUNNING)
/// 2. Start heartbeat → build task input → execute task → stop heartbeat
/// 3. Phase 1 finalize: persist terminal state (with bounded retries)
///
/// Returns `Some(Phase2Work)` when the task reached a terminal state that
/// needs workflow advancement. Returns `None` for retried/aborted tasks.
///
/// The caller should release its semaphore permit BEFORE running Phase 2,
/// so that slow workflow callbacks do not consume execution concurrency.
pub(crate) async fn execute_and_finalize(
    broker: Arc<PostgresBroker>,
    task_fn: RegisteredTask,
    row: ClaimedTaskRow,
    worker_id: String,
    hostname: String,
    recovery: RecoveryConfig,
    payload_policy: PayloadPolicy,
) -> Option<Phase2Work> {
    let task_id = row.id;
    let queue_name = row.queue_name.clone();
    let is_workflow_task = row.is_workflow_task;
    let pid = std::process::id() as i32;
    let accepts_workflow_ctx = task_fn.accepts_workflow_ctx();
    // The stored options are read once, here, into typed values; the run
    // limit and the retry decision use this value and never read the text
    // again.
    let stored_options = read_stored_options(row.task_options.as_deref());

    // Phase 0: Confirm ownership and transition to RUNNING.
    let running = match confirm_ownership_and_set_running(
        &broker,
        task_id,
        &worker_id,
        pid,
        &hostname,
        is_workflow_task,
        row.claimed_at,
        recovery.auto_terminate_orphaned_workflow_tasks,
    )
    .await
    {
        OwnershipOutcome::Running(running) => running,
        OwnershipOutcome::ExpiredBeforeStart => {
            return Some(Phase2Work {
                task_id,
                is_success: false,
                queue_name,
                is_workflow_task,
                capacity_notified: false,
            });
        }
        OwnershipOutcome::Aborted => return None,
    };

    // Runner heartbeats: the worker's runner-beat writer beats this row while
    // the dispatch holds it (`runner_beats::LiveDispatches`); the first beat
    // is written with the RUNNING transition above.

    // Phase 1: Build the task input + execute task. The deadline (if any) is measured
    // around user-code execution; on expiry the result is TASK_TIMEOUT and the
    // finalize path below decides fail-vs-retry via auto_retry_for. Stored
    // options that do not fit the rule that wrote them fail the task here,
    // after the row is RUNNING and before user code runs (Python
    // `core/worker/stored_timeout.py`).
    let timeout = match stored_options.run_permit() {
        RunPermit::RunWithoutTimeout => Ok(None),
        RunPermit::RunWithTimeout(limit) => Ok(Some(limit)),
        RunPermit::Refuse(reason) => Err(TaskError::builtin(
            OperationalErrorCode::WorkerSerializationError,
            reason,
        )),
    };
    let prepared = timeout.and_then(|timeout| {
        build_claimed_task_input(&row, accepts_workflow_ctx).map(|task_input| (task_input, timeout))
    });
    let result = match prepared {
        Ok((task_input, timeout)) => execute_task(task_fn, task_input, timeout).await,
        Err(err) => TaskResult::Err(err),
    };

    // Stamp finalization handoff so the stale-RUNNING reaper skips this row
    // while finalize runs (the runner heartbeat has now stopped). Best-effort:
    // a failed stamp does not abort finalize — phase-1 CAS still protects
    // correctness. Mirrors Python's finalizing_at / finalizing_by_worker_id.
    match mark_task_finalizing(broker.pool(), task_id, &worker_id, row.claimed_at).await {
        Ok(FinalizingMark::NotHeld) => {
            tracing::debug!(task_id = %task_id, "finalizing handoff skipped: claim changed");
        }
        Ok(FinalizingMark::Marked) => {}
        Err(e) => {
            tracing::warn!(task_id = %task_id, error = %e, "failed to stamp finalizing handoff");
        }
    }

    // Finalize Phase 1: Persist terminal state (with phase-aware retry).
    let phase1_outcome = retry_phase1(
        &broker,
        task_id,
        result,
        &row,
        &stored_options,
        &running,
        &worker_id,
        &payload_policy,
    )
    .await;

    match phase1_outcome {
        Some(FinalizeOutcome::Terminal {
            is_success,
            capacity_notified,
        }) => Some(Phase2Work {
            task_id,
            is_success,
            queue_name,
            is_workflow_task,
            capacity_notified,
        }),
        Some(FinalizeOutcome::Retried | FinalizeOutcome::Finalized | FinalizeOutcome::Refused)
        | None => None,
    }
}

/// Run Phase 2 finalize: workflow advancement + capacity notifications.
///
/// Should be called AFTER the semaphore permit is released.
/// Uses bounded retries (up to 5) with reload-from-persisted-result for replays.
pub(crate) async fn run_phase2(
    pool: &sqlx::PgPool,
    workflow_registry: &WorkflowSpecRegistry,
    work: Phase2Work,
    payload_policy: &PayloadPolicy,
    retention: &RetentionConfig,
) {
    retry_phase2(
        pool,
        workflow_registry,
        work.task_id,
        work.is_success,
        &work.queue_name,
        work.is_workflow_task,
        work.capacity_notified,
        payload_policy,
        retention,
    )
    .await;
}

/// Run Phase 1: the first write and up to [`PHASE1_MAX_RETRIES`] retries of
/// it, each after [`phase_retry_delay`]. Only a retryable error is retried;
/// each try sends one statement (Python `_handle_finalize_error`, stage
/// PHASE1).
///
/// Returns `Some(outcome)` when a write answered, `None` when the error is
/// not retryable or the retries are spent; the row then stays RUNNING for
/// the reaper.
#[allow(clippy::too_many_arguments)]
async fn retry_phase1(
    broker: &PostgresBroker,
    task_id: Uuid,
    result: TaskResult<Vec<u8>>,
    row: &ClaimedTaskRow,
    stored_options: &StoredOptions,
    running: &SetRunningRow,
    worker_id: &str,
    payload_policy: &PayloadPolicy,
) -> Option<FinalizeOutcome> {
    for attempt in 0..=PHASE1_MAX_RETRIES {
        if attempt > 0 {
            let delay = phase_retry_delay(attempt);
            tracing::warn!(
                task_id = %task_id,
                attempt,
                max = PHASE1_MAX_RETRIES,
                delay_s = format!("{:.1}", delay),
                "retrying Phase 1 finalize",
            );
            tokio::time::sleep(Duration::from_secs_f64(delay)).await;
        }
        match persist_terminal_state(
            broker,
            task_id,
            result.clone(),
            row,
            stored_options,
            running,
            worker_id,
            payload_policy,
        )
        .await
        {
            Ok(outcome) => return Some(outcome),
            Err(e) if !e.retryable => {
                tracing::error!(
                    task_id = %task_id,
                    error = %e,
                    attempt,
                    "Phase 1 finalize failed (non-retryable) — task stays RUNNING for reaper",
                );
                return None;
            }
            Err(e) => {
                tracing::warn!(
                    task_id = %task_id,
                    error = %e,
                    attempt,
                    "Phase 1 finalize failed (retryable)",
                );
            }
        }
    }

    tracing::error!(
        task_id = %task_id,
        retries = PHASE1_MAX_RETRIES,
        "Phase 1 finalize retries exhausted — task stays RUNNING for reaper",
    );
    None
}

/// Run Phase 2 with bounded retries.
///
/// Phase 1 already committed the durable outbox row, so every retry repeats
/// the same consume-and-progress transaction without consulting live rows.
#[allow(clippy::too_many_arguments)]
async fn retry_phase2(
    pool: &sqlx::PgPool,
    workflow_registry: &WorkflowSpecRegistry,
    task_id: Uuid,
    is_success: bool,
    queue_name: &str,
    is_workflow_task: bool,
    capacity_notified: bool,
    payload_policy: &PayloadPolicy,
    retention: &RetentionConfig,
) {
    // First attempt consumes the durable outbox evidence.
    match finalize_workflow_phase(
        pool,
        workflow_registry,
        task_id,
        is_success,
        queue_name,
        is_workflow_task,
        capacity_notified,
        payload_policy,
        retention,
    )
    .await
    {
        Ok(()) => return,
        Err(e) if !e.retryable => {
            tracing::error!(
                task_id = %task_id,
                error = %e,
                "Phase 2 finalize failed (non-retryable)",
            );
            return;
        }
        Err(e) => {
            tracing::warn!(
                task_id = %task_id,
                error = %e,
                "Phase 2 finalize failed, scheduling retries",
            );
        }
    }

    // Retry attempts consume the same durable pending evidence. If a previous
    // attempt committed, consume reports PENDING_ABSENT and the call is a no-op.
    for attempt in 1..=PHASE2_MAX_RETRIES {
        let delay = phase_retry_delay(attempt);
        tracing::warn!(
            task_id = %task_id,
            attempt,
            max = PHASE2_MAX_RETRIES,
            delay_s = format!("{:.1}", delay),
            "retrying Phase 2 finalize",
        );
        tokio::time::sleep(Duration::from_secs_f64(delay)).await;

        match finalize_workflow_phase(
            pool,
            workflow_registry,
            task_id,
            is_success,
            queue_name,
            is_workflow_task,
            capacity_notified,
            payload_policy,
            retention,
        )
        .await
        {
            Ok(()) => return,
            Err(e) if !e.retryable => {
                tracing::error!(
                    task_id = %task_id,
                    error = %e,
                    attempt,
                    "Phase 2 finalize retry failed (non-retryable)",
                );
                return;
            }
            Err(e) => {
                tracing::warn!(
                    task_id = %task_id,
                    error = %e,
                    attempt,
                    "Phase 2 finalize retry failed (retryable)",
                );
            }
        }
    }

    tracing::error!(
        task_id = %task_id,
        attempts = PHASE2_MAX_RETRIES,
        "Phase 2 finalize retries exhausted — workflow recovery will handle",
    );
}

/// Compute phase-level retry delay with exponential backoff.
pub(crate) fn phase_retry_delay(attempt: u32) -> f64 {
    (PHASE_RETRY_BASE_DELAY_S * 2.0f64.powi(attempt as i32 - 1)).min(PHASE_RETRY_MAX_DELAY_S)
}

// ---------------------------------------------------------------------------
// Workflow context parsing (private helper)
// ---------------------------------------------------------------------------

mod parse {
    use std::collections::HashMap;

    use crate::core::task::error::{OperationalErrorCode, TaskError, TaskErrorKind};
    use crate::core::task::result::TaskResult;
    use crate::core::workflow::context::WorkflowContext;
    use crate::core::workflow::SubWorkflowSummary;
    use serde::Deserialize;
    use uuid::Uuid;

    /// The context payload in Python's shape (`core/workflows/engine.py:1700-1721`). `definition_key_by_id`
    /// is not read: the receiving task decodes each result with its own type.
    #[derive(Debug, Deserialize)]
    struct WorkflowCtxPayload {
        workflow_id: Uuid,
        task_index: i32,
        task_name: String,
        #[serde(default)]
        results_by_id: HashMap<String, serde_json::Value>,
        #[serde(default)]
        task_name_by_id: HashMap<String, serde_json::Value>,
        #[serde(default)]
        summaries_by_id: HashMap<String, serde_json::Value>,
    }

    /// Read the context payload (Python `core/worker/child_runner.py:1654-1766`).
    ///
    /// - A result entry that is `null`, that has no task name, or that is not a result envelope becomes a
    ///   `RESULT_DESERIALIZATION_ERROR` entry naming the node; the other entries stay readable.
    /// - A summary entry is the stored summary text; one that does not parse is `WORKER_SERIALIZATION_ERROR`
    ///   for the whole task. An entry that is not text is skipped.
    /// - A payload of another shape is `WORKER_SERIALIZATION_ERROR`.
    pub(crate) fn parse_workflow_ctx(
        value: serde_json::Value,
    ) -> Result<WorkflowContext, TaskError> {
        let payload: WorkflowCtxPayload = serde_json::from_value(value).map_err(|e| {
            TaskError::builtin(
                OperationalErrorCode::WorkerSerializationError,
                format!("failed to parse workflow context payload: {}", e),
            )
        })?;

        let mut results_by_id: HashMap<String, TaskResult<serde_json::Value>> =
            HashMap::with_capacity(payload.results_by_id.len());
        for (node_id, envelope) in payload.results_by_id {
            let entry = context_entry(&node_id, envelope, payload.task_name_by_id.get(&node_id));
            results_by_id.insert(node_id, entry);
        }

        let mut summaries_by_id: HashMap<String, SubWorkflowSummary> =
            HashMap::with_capacity(payload.summaries_by_id.len());
        for (node_id, summary_json) in payload.summaries_by_id {
            let serde_json::Value::String(summary_text) = summary_json else {
                continue;
            };
            let parsed: SubWorkflowSummary = serde_json::from_str(&summary_text).map_err(|e| {
                TaskError::builtin(
                    OperationalErrorCode::WorkerSerializationError,
                    format!(
                        "failed to parse workflow context summary for node_id '{}': {}",
                        node_id, e,
                    ),
                )
            })?;
            summaries_by_id.insert(node_id, parsed);
        }

        Ok(WorkflowContext::new(
            payload.workflow_id,
            payload.task_index,
            payload.task_name,
            results_by_id,
            summaries_by_id,
        ))
    }

    /// One result entry of the context, with Python's sentinel errors (`core/worker/child_runner.py:1666-1739`).
    fn context_entry(
        node_id: &str,
        envelope: serde_json::Value,
        source_task_name: Option<&serde_json::Value>,
    ) -> TaskResult<serde_json::Value> {
        let unreadable =
            |message: String, data: serde_json::Value| -> TaskResult<serde_json::Value> {
                TaskResult::Err(TaskError {
                    exception: None,
                    error_code: OperationalErrorCode::ResultDeserializationError.into(),
                    data: Some(data),
                    message: Some(message),
                    kind: TaskErrorKind::Plain,
                })
            };
        if envelope.is_null() {
            return unreadable(
                format!(
                    "workflow_ctx result for node '{node_id}' unavailable (source task not registered at encode time)"
                ),
                serde_json::json!({ "node_id": node_id }),
            );
        }
        let Some(serde_json::Value::String(source_task_name)) = source_task_name else {
            return unreadable(
                format!("workflow_ctx missing task_name for node '{node_id}'; cannot decode"),
                serde_json::json!({ "node_id": node_id }),
            );
        };
        match serde_json::from_value::<TaskResult<serde_json::Value>>(envelope) {
            Ok(result) => result,
            Err(e) => unreadable(
                format!("decode_task_result failed for workflow_ctx node '{node_id}': {e}"),
                serde_json::json!({ "node_id": node_id, "source_task_name": source_task_name }),
            ),
        }
    }
}

#[cfg(test)]
mod result_wrap_tests {
    //! P6: the ok-result wrap embeds the task's result bytes verbatim via
    //! `&RawValue` — no intermediate `Value` tree, and no reformatting.

    use crate::core::task::result::TaskResult;

    #[test]
    fn raw_value_wrap_embeds_result_bytes_verbatim() {
        // A number form the old parse-to-Value path would have reformatted
        // ("1.2300" -> 1.23) must survive byte-for-byte inside the wrapper.
        let result_bytes: &[u8] = br#"{"amount": 1.2300, "id": 7}"#;
        let raw = serde_json::from_slice::<&serde_json::value::RawValue>(result_bytes)
            .expect("valid JSON");
        let wrapped_json = serde_json::to_string(&TaskResult::Ok(raw)).expect("wrap serializes");

        assert_eq!(
            wrapped_json,
            r#"{"__h_task_result__":true,"ok":{"amount": 1.2300, "id": 7},"err":null}"#,
            "result bytes must be embedded verbatim",
        );

        // And the wrapper still parses back through the typed reader.
        let reparsed: TaskResult<serde_json::Value> =
            serde_json::from_str(&wrapped_json).expect("wrapper round-trips");
        assert!(reparsed.is_ok());
    }

    #[test]
    fn invalid_result_bytes_are_rejected_like_the_value_parse() {
        // `&RawValue` parsing validates full JSON syntax, matching the previous
        // `from_slice::<Value>` gate: trailing garbage and truncation both fail.
        assert!(serde_json::from_slice::<&serde_json::value::RawValue>(b"{\"a\": 1} x").is_err());
        assert!(serde_json::from_slice::<&serde_json::value::RawValue>(b"{\"a\": ").is_err());
    }
}

#[cfg(test)]
mod task_input_tests {
    use super::build_task_input;
    use crate::core::workflow::context::WORKFLOW_CTX_KWARG;

    fn task_input(input: &str) -> String {
        let bytes = build_task_input(input, false).expect("task input built");
        String::from_utf8(bytes).expect("task input is UTF-8")
    }

    #[test]
    fn null_input_passes_as_null() {
        assert_eq!(task_input("null"), "null");
    }

    #[test]
    fn array_and_object_inputs_pass_as_written() {
        assert_eq!(task_input("[1, 2]"), "[1, 2]");
        assert_eq!(task_input(r#"{"a": 1}"#), r#"{"a": 1}"#);
        assert_eq!(task_input("{}"), "{}");
    }

    #[test]
    fn the_context_member_is_removed_for_a_task_without_context() {
        let input = format!(r#"{{"a":1,"{WORKFLOW_CTX_KWARG}":{{"ignored":true}}}}"#);
        let value: serde_json::Value = serde_json::from_str(&task_input(&input)).unwrap();
        assert_eq!(value, serde_json::json!({"a": 1}));
    }

    #[test]
    fn input_text_that_breaks_the_strict_read_rule_is_refused() {
        for input in ["{", "NaN", r#"{"a":1,"a":2}"#] {
            let err = build_task_input(input, false).unwrap_err();
            assert!(
                err.message
                    .unwrap_or_default()
                    .contains("stored input refused"),
                "{input}"
            );
        }
    }

    #[test]
    fn a_long_integer_passes_byte_for_byte() {
        let input = r#"{"k":123456789012345678901234567890}"#;
        assert_eq!(task_input(input), input);
    }

    fn refusal(input: &str) -> String {
        let err = build_task_input(input, false).unwrap_err();
        assert_eq!(err.error_code.to_string(), "WORKER_SERIALIZATION_ERROR");
        err.message.unwrap_or_default()
    }

    fn task_input_value(input: &str) -> serde_json::Value {
        serde_json::from_str(&task_input(input)).unwrap()
    }

    /// Python `core/worker/child_runner.py:1566-1612`: the receiving task gets the `inner` envelope of an
    /// `args_from` entry.
    #[test]
    fn an_args_from_entry_is_replaced_by_its_inner_envelope() {
        let input = r#"{"x": 1, "dep": {"__h_taskresult_envelope__": true, "source_task_name": "produce",
            "source_definition_key": null, "inner": {"__h_task_result__": true, "ok": 7, "err": null}}}"#;
        assert_eq!(
            task_input_value(input),
            serde_json::json!({
                "x": 1,
                "dep": {"__h_task_result__": true, "ok": 7, "err": null},
            }),
        );
    }

    #[test]
    fn a_malformed_args_from_entry_is_refused() {
        for entry in [
            r#"{"__h_taskresult_envelope__": true, "inner": null}"#,
            r#"{"__h_taskresult_envelope__": true, "source_task_name": "p", "source_definition_key": 3, "inner": null}"#,
            r#"{"__h_taskresult_envelope__": true, "source_task_name": "p", "source_definition_key": null}"#,
            r#"{"__h_taskresult_envelope__": true, "source_task_name": "p", "source_definition_key": null, "inner": null, "x": 1}"#,
        ] {
            let message = refusal(&format!(r#"{{"dep": {entry}}}"#));
            assert!(
                message.contains("args_from envelope for kwarg 'dep'"),
                "{message}"
            );
        }
    }

    /// Python's child removes the metadata kwarg before the call (`core/worker/child_runner.py:1641-1642`).
    #[test]
    fn the_workflow_meta_kwarg_is_removed() {
        let input = r#"{"x": 1, "__h_workflow_meta__": {"workflow_id": "w", "task_index": 0, "task_name": "t"}}"#;
        assert_eq!(task_input_value(input), serde_json::json!({"x": 1}));
    }

    /// Python `decode_kwargs` and `decode_value`: an engine key in a user value, or as a kwarg name, is
    /// refused; so is a value deeper than 200 levels. A non-object input is one value under the same rules.
    #[test]
    fn user_input_follows_the_user_value_rules() {
        for input in [
            r#"{"x": {"__h_task_result__": true}}"#,
            r#"{"x": [{"__builtin_task_code__": "X"}]}"#,
            r#"{"__horsies_workflow_ctx__": {}}"#,
            r#"{"__h_other__": 1}"#,
            r#"[{"__h_x": 1}]"#,
        ] {
            assert!(refusal(input).contains("reserved key"), "{input}");
        }
        let nested = |depth: usize| format!("{}{}", "[".repeat(depth), "]".repeat(depth));
        let deep = format!(
            r#"{{"x": {}}}"#,
            nested(crate::core::codec::MAX_VALUE_JSON_DEPTH + 1)
        );
        assert!(refusal(&deep).contains("nested deeper than 200"));
        let at_bound = format!(
            r#"{{"x": {}}}"#,
            nested(crate::core::codec::MAX_VALUE_JSON_DEPTH)
        );
        assert_eq!(task_input(&at_bound), at_bound);
    }

    /// The context kwarg is Python's key; at check time (no runtime context) it is dropped.
    #[test]
    fn the_context_kwarg_is_pythons_key() {
        assert_eq!(WORKFLOW_CTX_KWARG, "__h_workflow_ctx__");
        let input = r#"{"x": 1, "__h_workflow_ctx__": {"workflow_id": "00000000-0000-0000-0000-000000000000",
            "task_index": 0, "task_name": "t"}}"#;
        assert_eq!(task_input_value(input), serde_json::json!({"x": 1}));
    }
}

#[cfg(test)]
mod workflow_ctx_tests {
    use super::parse_workflow_ctx;
    use crate::core::task::result::TaskResult;
    use crate::core::workflow::node::NodeKey;

    /// Python's payload (`core/workflows/engine.py:1700-1721`): envelope objects or null, the task name and
    /// the definition key of each node, and the stored summary text.
    #[test]
    fn pythons_context_payload_reads() {
        let summary = r#"{"status":"COMPLETED","output":5,"total_tasks":1,"completed_tasks":1,"failed_tasks":0,"skipped_tasks":0,"error_summary":null}"#;
        let payload = serde_json::json!({
            "workflow_id": "00000000-0000-0000-0000-000000000000",
            "task_index": 2,
            "task_name": "consume",
            "results_by_id": {
                "a": {"__h_task_result__": true, "ok": 41, "err": null},
                "b": null,
                "c": {"__h_task_result__": true, "ok": 1, "err": null},
                "d": {"not": "an envelope"},
                "sub": {"__h_task_result__": true, "ok": 5, "err": null},
            },
            "task_name_by_id": {"a": "produce", "b": "produce", "d": "produce", "sub": "__sub_workflow:child"},
            "definition_key_by_id": {"a": null, "b": null, "c": null, "d": null, "sub": "app.child"},
            "summaries_by_id": {"sub": summary},
        });
        let ctx = parse_workflow_ctx(payload).unwrap();
        let value: TaskResult<i32> = ctx.result_for(&NodeKey::new("a".to_owned())).unwrap();
        assert_eq!(value.unwrap(), 41);
        for (node, message) in [
            ("b", "unavailable"),
            ("c", "missing task_name"),
            ("d", "decode_task_result failed"),
        ] {
            let entry: TaskResult<i32> = ctx.result_for(&NodeKey::new(node.to_owned())).unwrap();
            let error = entry.unwrap_err();
            assert_eq!(error.error_code.to_string(), "RESULT_DESERIALIZATION_ERROR");
            assert!(error.message.unwrap().contains(message), "{node}");
        }
        let summary = ctx
            .summary_for(&NodeKey::<i32>::new("sub".to_owned()))
            .unwrap();
        assert_eq!(summary.output, Some(serde_json::json!(5)));
    }

    #[test]
    fn a_summary_text_that_does_not_parse_fails_the_task() {
        let payload = serde_json::json!({
            "workflow_id": "00000000-0000-0000-0000-000000000000",
            "task_index": 0,
            "task_name": "t",
            "summaries_by_id": {"sub": "{not json"},
        });
        let error = parse_workflow_ctx(payload).unwrap_err();
        assert_eq!(error.error_code.to_string(), "WORKER_SERIALIZATION_ERROR");
    }
}

#[cfg(test)]
mod set_running_gate_tests {
    //! P1: `confirm_ownership_and_set_running` gates the workflow_task RUNNING
    //! UPDATE on `is_workflow_task` to drop a per-task-start round trip for plain
    //! tasks. Cross-check: the update must STILL happen for workflow tasks.
    use super::{confirm_ownership_and_set_running, test_uuid, OwnershipOutcome};
    use crate::broker::PostgresBroker;
    use serial_test::serial;
    use sqlx::PgPool;
    use uuid::Uuid;

    async fn test_broker() -> PostgresBroker {
        PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await)
    }

    async fn seed_claimed(pool: &PgPool, task_id: &str, is_wf: bool) {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint, idempotency_key_digest,
                status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
                started_at, finalizing_at, finalizing_by_worker_id,
                worker_pid, worker_hostname, worker_process_name,
                retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
            ) VALUES (
                $1, 'p1_task', 'default', 100, 'null', NULL, 'ISOLATED',
                'standard_30d', NOW(), NOW(),
                CASE WHEN $2 THEN NULL ELSE 1 END, CASE WHEN $2 THEN NULL ELSE decode(repeat('00', 32), 'hex') END, NULL,
                'CLAIMED', NOW(), NOW(), 'w1', NOW() + interval '1 minute',
                NULL, NULL, NULL,
                NULL, NULL, NULL,
                0, 3, NULL, NULL, $2,
                NOW(), FALSE, NULL, NULL
            )",
        )
        .bind(test_uuid(task_id))
        .bind(is_wf)
        .execute(pool)
        .await
        .expect("seed task");
    }

    /// The claim generation the seeded row carries.
    async fn generation(pool: &PgPool, task_id: Uuid) -> chrono::DateTime<chrono::Utc> {
        sqlx::query_scalar("SELECT claimed_at FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(pool)
            .await
            .expect("seeded claim generation")
    }

    #[tokio::test]
    #[serial]
    async fn workflow_task_still_transitions_node_to_running() {
        let broker = test_broker().await;
        let pool = broker.pool().clone();
        let wf_id = Uuid::new_v4().to_string();
        let task_id = Uuid::new_v4().to_string();

        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index, definition_key, depth,
                root_workflow_id, sent_at, created_at, started_at, updated_at
            ) VALUES ($1, 'p1_wf', 'RUNNING', 'fail', NULL, 'test.p1.v1', 0, $1,
                      NOW(), NOW(), NOW(), NOW())",
        )
        .bind(test_uuid(&wf_id))
        .execute(&pool)
        .await
        .expect("insert workflow");
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, task_id,
                created_at, max_retries, execution, retention_class_key
            ) VALUES (
                $1, $2, 0, 'node_0', 'p1_task', 'null', 'default', 100, '{}', FALSE, 'all',
                'ENQUEUED', FALSE, $3, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(test_uuid(&wf_id))
        .bind(test_uuid(&task_id))
        .execute(&pool)
        .await
        .expect("insert node");
        seed_claimed(&pool, &task_id, true).await;
        let claimed_at = generation(&pool, test_uuid(&task_id)).await;

        let outcome = confirm_ownership_and_set_running(
            &broker,
            Uuid::parse_str(&task_id).unwrap(),
            "w1",
            1,
            "h1",
            true,
            claimed_at,
            false,
        )
        .await;
        assert!(matches!(outcome, OwnershipOutcome::Running(_)));

        let (node_status, node_started_at): (String, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT status, started_at FROM horsies_workflow_tasks WHERE workflow_id = $1",
            )
            .bind(test_uuid(&wf_id))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            node_status, "RUNNING",
            "workflow node must transition to RUNNING (gate must not skip it)"
        );
        assert!(
            node_started_at.is_some(),
            "first RUNNING handoff must stamp workflow-node started_at",
        );

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(&task_id))
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(test_uuid(&wf_id))
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[serial]
    async fn running_replay_preserves_the_first_node_start() {
        let broker = test_broker().await;
        let pool = broker.pool().clone();
        let wf_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let first_started_at = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(
            chrono::Utc::now().timestamp_micros() - 3_600_000_000,
        )
        .unwrap();

        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index, definition_key, depth,
                root_workflow_id, sent_at, created_at, started_at, updated_at
            ) VALUES ($1, 'p1_replay_wf', 'RUNNING', 'fail', NULL,
                      'test.p1.replay.v1', 0, $1, NOW(), NOW(), NOW(), NOW())",
        )
        .bind(wf_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, task_id,
                started_at, created_at, max_retries, execution, retention_class_key
            ) VALUES (
                $1, $2, 0, 'node_0', 'p1_task', 'null', 'default', 100, '{}', FALSE, 'all',
                'RUNNING', FALSE, $3, $4, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(wf_id)
        .bind(task_id)
        .bind(first_started_at)
        .execute(&pool)
        .await
        .unwrap();
        seed_claimed(&pool, &task_id.to_string(), true).await;
        let claimed_at = generation(&pool, task_id).await;

        let before: String = sqlx::query_scalar(
            "SELECT ctid::text FROM horsies_workflow_tasks WHERE workflow_id = $1",
        )
        .bind(wf_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let outcome = confirm_ownership_and_set_running(
            &broker, task_id, "w1", 1, "h1", true, claimed_at, false,
        )
        .await;
        assert!(matches!(outcome, OwnershipOutcome::Running(_)));

        let persisted: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "SELECT started_at FROM horsies_workflow_tasks WHERE workflow_id = $1",
        )
        .bind(wf_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(persisted, first_started_at);
        let after: String = sqlx::query_scalar(
            "SELECT ctid::text FROM horsies_workflow_tasks WHERE workflow_id = $1",
        )
        .bind(wf_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            before, after,
            "RUNNING replay must not create a new node tuple"
        );

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(wf_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(wf_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[serial]
    async fn plain_task_starts_running_without_workflow_update() {
        let broker = test_broker().await;
        let pool = broker.pool().clone();
        let task_id = Uuid::new_v4().to_string();
        seed_claimed(&pool, &task_id, false).await;
        let claimed_at = generation(&pool, test_uuid(&task_id)).await;

        let outcome = confirm_ownership_and_set_running(
            &broker,
            Uuid::parse_str(&task_id).unwrap(),
            "w1",
            1,
            "h1",
            false,
            claimed_at,
            false,
        )
        .await;
        assert!(matches!(outcome, OwnershipOutcome::Running(_)));

        let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(&task_id))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "RUNNING");

        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(test_uuid(&task_id))
            .execute(&pool)
            .await
            .ok();
    }
    #[tokio::test]
    #[serial]
    async fn handoff_preserves_link_states_and_observes_terminal_nodes() {
        let broker = test_broker().await;
        let pool = broker.pool().clone();
        for status in [
            Some("ENQUEUED"),
            Some("READY"),
            Some("PENDING"),
            Some("RUNNING"),
            Some("COMPLETED"),
            Some("FAILED"),
            Some("SKIPPED"),
            None,
        ] {
            let wf = Uuid::new_v4();
            let task = Uuid::new_v4();
            let node = Uuid::new_v4();
            sqlx::query("INSERT INTO horsies_workflows(id,name,status,definition_key,on_error,depth,root_workflow_id,sent_at,created_at,updated_at) VALUES($1,'handoff_contract','RUNNING','handoff_contract','fail',0,$1,NOW(),NOW(),NOW())").bind(wf).execute(&pool).await.unwrap();
            if let Some(status) = status {
                sqlx::query("INSERT INTO horsies_workflow_tasks(id,workflow_id,task_index,task_name,input,status,task_id,started_at,queue_name,priority,dependencies,allow_failed_deps,join_type,is_subworkflow,created_at,max_retries,execution,retention_class_key) VALUES($1,$2,0,'handoff_contract','null',$3,$4,timestamptz '2026-01-01','default',100,'{}',FALSE,'all',FALSE,NOW(),0,'ISOLATED','standard_30d')").bind(node).bind(wf).bind(status).bind(task).execute(&pool).await.unwrap();
            }
            seed_claimed(&pool, &task.to_string(), true).await;
            let claimed_at = generation(&pool, task).await;
            let result = confirm_ownership_and_set_running(
                &broker, task, "w1", 1, "h1", true, claimed_at, true,
            )
            .await;
            let outcome = match result {
                OwnershipOutcome::Running(_) => "running",
                OwnershipOutcome::Aborted => "aborted",
                OwnershipOutcome::ExpiredBeforeStart => "expired",
            };
            let persisted: Option<String> =
                sqlx::query_scalar("SELECT status FROM horsies_workflow_tasks WHERE id=$1")
                    .bind(node)
                    .fetch_optional(&pool)
                    .await
                    .unwrap();
            let task_state: Option<String> =
                sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id=$1")
                    .bind(task)
                    .fetch_optional(&pool)
                    .await
                    .unwrap();
            let history: Option<String> =
                sqlx::query_scalar("SELECT status FROM horsies_task_history WHERE task_id=$1")
                    .bind(task)
                    .fetch_optional(&pool)
                    .await
                    .unwrap();
            match status {
                Some("ENQUEUED" | "READY" | "PENDING" | "RUNNING") => {
                    assert_eq!(outcome, "running");
                    assert_eq!(persisted.as_deref(), Some("RUNNING"));
                    assert_eq!(task_state.as_deref(), Some("RUNNING"));
                    assert!(history.is_none());
                }
                _ => {
                    assert_eq!(outcome, "aborted");
                    assert_eq!(persisted.as_deref(), status);
                    assert!(task_state.is_none());
                    assert_eq!(history.as_deref(), Some("CANCELLED"));
                }
            }
            clean_handoff_case(&pool, wf, task).await;
        }
        let wf = Uuid::new_v4();
        let task = Uuid::new_v4();
        let node = Uuid::new_v4();
        sqlx::query("INSERT INTO horsies_workflows(id,name,status,definition_key,on_error,depth,root_workflow_id,sent_at,created_at,updated_at) VALUES($1,'handoff_race','RUNNING','handoff_contract','fail',0,$1,NOW(),NOW(),NOW())").bind(wf).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO horsies_workflow_tasks(id,workflow_id,task_index,task_name,input,status,task_id,queue_name,priority,dependencies,allow_failed_deps,join_type,is_subworkflow,created_at,max_retries,execution,retention_class_key) VALUES($1,$2,0,'handoff_race','null','ENQUEUED',$3,'default',100,'{}',FALSE,'all',FALSE,NOW(),0,'ISOLATED','standard_30d')").bind(node).bind(wf).bind(task).execute(&pool).await.unwrap();
        seed_claimed(&pool, &task.to_string(), true).await;
        let claimed_at = generation(&pool, task).await;
        let mut held = pool.begin().await.unwrap();
        sqlx::query("UPDATE horsies_workflow_tasks SET status='FAILED' WHERE id=$1")
            .bind(node)
            .execute(held.as_mut())
            .await
            .unwrap();
        let race_broker = PostgresBroker::from_pool(pool.clone());
        let handoff = tokio::spawn(async move {
            confirm_ownership_and_set_running(
                &race_broker,
                task,
                "w1",
                1,
                "h1",
                true,
                claimed_at,
                true,
            )
            .await
        });
        let mut waited = false;
        for _ in 0..500 {
            waited=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>pg_backend_pid())").fetch_one(&pool).await.unwrap();
            if waited {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(waited, "handoff must wait for the node update");
        held.commit().await.unwrap();
        let result = handoff.await.unwrap();
        assert!(matches!(result, OwnershipOutcome::Aborted));
        let node_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflow_tasks WHERE id=$1")
                .bind(node)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(node_status, "FAILED");
        clean_handoff_case(&pool, wf, task).await;
    }

    async fn clean_handoff_case(pool: &PgPool, workflow: Uuid, task: Uuid) {
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(task)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow)
            .execute(pool)
            .await
            .unwrap();
    }
}

#[cfg(test)]
#[path = "finalizing_tests.rs"]
mod finalizing_tests;

#[cfg(test)]
mod finalize_spec_tests;

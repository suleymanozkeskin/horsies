//! Task execution phase decomposition.
//!
//! Mirrors Python's `_finalize_after` architecture with named phase boundaries
//! and phase-aware error recovery:
//!
//! - **Phase 0** — `confirm_ownership_and_set_running`: CLAIMED → RUNNING
//! - **Phase 1a** — `build_task_envelope`: arg/kwargs deserialization
//! - **Phase 1b** — `execute_task`: async/blocking invocation with panic isolation
//! - **Phase 2a** — `schedule_retry_for_task`: atomic requeue + attempt record
//! - **Finalize Phase 1** — `persist_terminal_state`: terminal DB write
//! - **Finalize Phase 2** — `finalize_workflow_phase`: workflow callback + NOTIFY
//!
//! Error recovery uses per-phase retry budgets (Phase 1: 3, Phase 2: 5) and
//! supports reload-from-persisted-result for Phase 2 replays.

use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::broker::{ClaimedTaskRow, PostgresBroker, SetRunningRow};
use crate::core::config::payload::{enforce_payload_policy, PayloadKind, PayloadPolicy};
use crate::core::config::recovery::RecoveryConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::task::error::{OperationalErrorCode, OutcomeCode, TaskError};
use crate::core::task::fn_trait::RegisteredTask;
use crate::core::task::result::TaskResult;
use crate::core::workflow::context::WORKFLOW_CTX_KWARG;

use crate::worker::heartbeat::spawn_runner_heartbeat;
use crate::worker::retry::{calculate_retry_delay, parse_timeout_ms, should_retry};

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
    ExpiredBeforeStart { result_json: String },
    /// Ownership lost or workflow stopped — skip execution.
    Aborted,
}

/// Outcome of Phase 1 finalize: persisting the terminal task state.
pub(crate) enum FinalizeOutcome {
    /// Task completed or failed terminally. Needs Phase 2 (workflow callback).
    Terminal {
        result_json: String,
        is_success: bool,
    },
    /// Task was requeued for retry. No workflow callback needed.
    Retried,
    /// Plain (non-workflow) ok task fully finalized in one statement (CAS +
    /// attempt + capacity notify). No Phase 2 needed. Parity with horsies PR #134.
    Finalized,
}

/// Data needed to run Phase 2 after the semaphore permit is released.
///
/// Returned by `execute_and_finalize` so the caller can drop the permit
/// before running potentially slow workflow callbacks + retries.
pub(crate) struct Phase2Work {
    pub task_id: String,
    pub result_json: String,
    pub is_success: bool,
    pub queue_name: String,
    pub is_workflow_task: bool,
}

/// Outcome of retry scheduling (mirrors Python's `_schedule_retry` return).
pub(crate) enum ScheduleRetryOutcome {
    /// Retry was scheduled and committed.
    Scheduled,
    /// Task is no longer RUNNING (reaper reclaimed).
    ReaperReclaimed,
    /// next_retry_at would exceed good_until.
    Expired,
    /// Transient DB error after exhausting retries.
    DbError,
}

pub(crate) struct WorkerProcessInfo<'a> {
    worker_id: &'a str,
    hostname: &'a str,
    pid: i32,
    process_name: &'a str,
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
    pub task_id: String,
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
// Finalize retry helper
// ---------------------------------------------------------------------------

/// Maximum retry attempts for the finalize transaction on transient DB errors.
pub(crate) const FINALIZE_MAX_RETRIES: u32 = 3;
/// Base delay for finalize retry backoff (doubles each attempt, capped at 15s).
const FINALIZE_RETRY_BASE_MS: u64 = 500;
/// Maximum delay for finalize retry backoff.
const FINALIZE_RETRY_MAX_MS: u64 = 15_000;

/// Phase 1 finalize gets 3 retries (matches Python `_FINALIZE_PHASE1_MAX_RETRIES`).
const PHASE1_MAX_RETRIES: u32 = 3;
/// Phase 2 finalize gets 5 retries (matches Python `_FINALIZE_PHASE2_MAX_RETRIES`).
const PHASE2_MAX_RETRIES: u32 = 5;
/// Base delay for phase-level retry backoff.
const PHASE_RETRY_BASE_DELAY_S: f64 = 0.5;
/// Maximum delay for phase-level retry backoff.
const PHASE_RETRY_MAX_DELAY_S: f64 = 15.0;

/// Run an async finalize closure with retry on transient DB errors.
pub(crate) async fn finalize_with_retry<T, F, Fut>(
    task_id: &str,
    label: &str,
    f: F,
) -> Result<T, crate::broker::BrokerError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, crate::broker::BrokerError>>,
{
    let mut last_err = None;
    for attempt in 0..FINALIZE_MAX_RETRIES {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if e.is_retryable() && attempt + 1 < FINALIZE_MAX_RETRIES => {
                let delay_ms =
                    (FINALIZE_RETRY_BASE_MS * 2u64.pow(attempt)).min(FINALIZE_RETRY_MAX_MS);
                tracing::warn!(
                    task_id = %task_id,
                    phase = label,
                    attempt = attempt + 1,
                    max = FINALIZE_MAX_RETRIES,
                    delay_ms,
                    error = %e,
                    "finalize transient error, retrying",
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                last_err = Some(e);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Err(last_err.expect("loop ran at least once"))
}

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
pub(crate) async fn confirm_ownership_and_set_running(
    broker: &PostgresBroker,
    task_id: &str,
    worker_id: &str,
    pid: i32,
    hostname: &str,
    is_workflow_task: bool,
    claimed_at: Option<chrono::DateTime<Utc>>,
) -> OwnershipOutcome {
    match broker
        .set_running(task_id, worker_id, pid, hostname, "worker", claimed_at)
        .await
    {
        Ok(Some(running)) => {
            // `set_running` has already durably committed the CLAIMED → RUNNING
            // flip on horsies_tasks. The workflow_task RUNNING status is purely
            // informational (COMPLETE_WORKFLOW_TASK_SQL accepts an ENQUEUED
            // workflow_task), so a transient failure of this update must NOT
            // fail the task closed before it ever runs. Log and proceed.
            //
            // Gate on `is_workflow_task`: for a plain task this UPDATE matches
            // zero rows, so running it unconditionally added a full round trip to
            // every task start's critical path (P1). The column is set at enqueue
            // (migration 0017) precisely to route workflow work without this cost.
            if is_workflow_task {
                if let Err(e) = sqlx::query(
                    "UPDATE horsies_workflow_tasks \
                     SET status = 'RUNNING' \
                     WHERE task_id = $1 AND status = 'ENQUEUED'",
                )
                .bind(task_id)
                .execute(broker.pool())
                .await
                {
                    tracing::warn!(
                        task_id = %task_id,
                        error = %e,
                        "failed to update workflow task to RUNNING; proceeding with execution \
                         (workflow_task left ENQUEUED, which completion tolerates)",
                    );
                }
            }
            OwnershipOutcome::Running(running)
        }
        Ok(None) => {
            match broker
                .expire_claimed_task_before_start(task_id, worker_id, claimed_at)
                .await
            {
                Ok(Some(result_json)) => {
                    tracing::info!(
                        task_id = %task_id,
                        "task expired before execution started",
                    );
                    return OwnershipOutcome::ExpiredBeforeStart { result_json };
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
                    if let Err(e) =
                        handle_workflow_stop_with_retry(broker, task_id, wf_status).await
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
    task_id: &str,
    workflow_status: &str,
) -> Result<(), crate::broker::BrokerError> {
    let mut last_err: Option<crate::broker::BrokerError> = None;

    for attempt in 1..=PRESTART_DB_RETRY_ATTEMPTS {
        match broker
            .handle_workflow_stop_before_start(task_id, workflow_status)
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
    task_id: &str,
    worker_id: &str,
    claimed_at: Option<chrono::DateTime<Utc>>,
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
// Phase 1a: Build task envelope (arg deserialization)
// ---------------------------------------------------------------------------

/// Deserialize args/kwargs from the claimed row and build the task invocation envelope.
///
/// Handles workflow context injection if the task function accepts it.
pub(crate) fn build_task_envelope(
    row: &ClaimedTaskRow,
    accepts_workflow_ctx: bool,
) -> Result<Vec<u8>, TaskError> {
    build_envelope_from_parts(row.args.as_deref(), row.kwargs.as_deref(), accepts_workflow_ctx)
}

/// Build the worker args/kwargs envelope from raw args/kwargs JSON strings.
///
/// Shared by [`build_task_envelope`] (execution) and `app.check()`'s
/// payload validation (schedules and workflow nodes) so check-time dry-runs
/// use the exact same args-coercion and kwargs-object rejection as execution.
///
/// `accepts_workflow_ctx` controls the `__horsies_workflow_ctx__` →
/// `workflow_ctx` rename; check-time callers pass `false` (no runtime context).
pub(crate) fn build_envelope_from_parts(
    args: Option<&str>,
    kwargs: Option<&str>,
    accepts_workflow_ctx: bool,
) -> Result<Vec<u8>, TaskError> {
    let args_value: serde_json::Value = match args {
        Some(json) => serde_json::from_str(json).map_err(|e| {
            TaskError::builtin(
                OperationalErrorCode::WorkerSerializationError,
                format!("failed to parse args JSON: {}", e),
            )
        })?,
        None => serde_json::Value::Null,
    };

    let args_array = match args_value {
        serde_json::Value::Null => Vec::new(),
        serde_json::Value::Array(arr) => arr,
        other => vec![other],
    };

    let kwargs_value: serde_json::Value = match kwargs {
        Some(json) => serde_json::from_str(json).map_err(|e| {
            TaskError::builtin(
                OperationalErrorCode::WorkerSerializationError,
                format!("failed to parse kwargs JSON: {}", e),
            )
        })?,
        None => serde_json::Value::Null,
    };

    let mut kwargs_object = match kwargs_value {
        serde_json::Value::Null => serde_json::Map::new(),
        serde_json::Value::Object(map) => map,
        _ => {
            return Err(TaskError::builtin(
                OperationalErrorCode::WorkerSerializationError,
                "kwargs payload is not a JSON object",
            ));
        }
    };

    if let Some(ctx_value) = kwargs_object.remove(WORKFLOW_CTX_KWARG) {
        if accepts_workflow_ctx {
            let workflow_ctx = parse_workflow_ctx(ctx_value)?;
            let ctx_json = serde_json::to_value(&workflow_ctx).map_err(|e| {
                TaskError::builtin(
                    OperationalErrorCode::WorkerSerializationError,
                    format!("failed to serialize workflow context: {}", e),
                )
            })?;
            kwargs_object.insert("workflow_ctx".to_owned(), ctx_json);
        }
    }

    let envelope = serde_json::json!({
        "args": args_array,
        "kwargs": kwargs_object,
    });

    serde_json::to_vec(&envelope).map_err(|e| {
        TaskError::builtin(
            OperationalErrorCode::WorkerSerializationError,
            format!("failed to serialize args/kwargs envelope: {}", e),
        )
    })
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
    envelope: Vec<u8>,
    timeout: Option<Duration>,
) -> TaskResult<Vec<u8>> {
    match task_fn {
        RegisteredTask::Async { task: f, .. } => {
            let mut handle = tokio::spawn(async move { f.execute(&envelope).await });
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
                    OperationalErrorCode::TaskError,
                    format!("async task panicked: {}", join_err),
                )),
            }
        }
        RegisteredTask::Blocking { task: f, .. } => {
            let handle = tokio::task::spawn_blocking(move || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f.execute(&envelope)))
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
                    OperationalErrorCode::TaskError,
                    "blocking task panicked".to_owned(),
                )),
                Err(join_err) => TaskResult::Err(TaskError::builtin(
                    OperationalErrorCode::TaskError,
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
// Retry scheduling
// ---------------------------------------------------------------------------

/// Schedule a task for retry: atomically requeue + record attempt, then spawn delayed NOTIFY.
///
/// Returns a named outcome matching Python's `_schedule_retry` contract:
/// - `Scheduled` — retry committed, delayed NOTIFY spawned
/// - `ReaperReclaimed` — task no longer RUNNING
/// - `Expired` — next_retry_at would exceed good_until
/// - `DbError` — transient DB failure after exhausting retries
pub(crate) async fn schedule_retry_for_task(
    broker: &PostgresBroker,
    task_id: &str,
    task_error: &TaskError,
    row: &ClaimedTaskRow,
    running: &SetRunningRow,
    attempt_num: i32,
    now: chrono::DateTime<Utc>,
    worker: &WorkerProcessInfo<'_>,
) -> ScheduleRetryOutcome {
    let task_started_at = running.started_at;
    let new_count = running.retry_count + 1;
    let delay = calculate_retry_delay(new_count as u32, row.task_options.as_deref());
    let next_retry_at = Utc::now() + chrono::Duration::milliseconds((delay * 1000.0) as i64);
    let error_code_str = task_error.error_code.as_ref().map(|c| c.to_string());
    let error_msg = task_error.message.as_deref();

    let tx_result = finalize_with_retry(task_id, "requeue", || async {
        let mut tx = broker
            .pool()
            .begin()
            .await
            .map_err(crate::broker::BrokerError::Database)?;
        let applied = broker
            .requeue_in_tx(
                &mut tx,
                task_id,
                Some(next_retry_at),
                worker.worker_id,
                Some(task_started_at),
            )
            .await?;
        if applied {
            broker
                .upsert_task_attempt(
                    &mut tx,
                    task_id,
                    attempt_num,
                    "FAILED",
                    true,
                    task_started_at,
                    now,
                    error_code_str.as_deref(),
                    error_msg,
                    None,
                    Some(worker.worker_id),
                    Some(worker.hostname),
                    Some(worker.pid),
                    Some(worker.process_name),
                )
                .await?;
        }
        tx.commit()
            .await
            .map_err(crate::broker::BrokerError::Database)?;
        Ok::<bool, crate::broker::BrokerError>(applied)
    })
    .await;

    match tx_result {
        Ok(true) => {
            // Spawn a delayed NOTIFY so the worker wakes up when the retry delay expires.
            let pool = broker.pool().clone();
            let queue = row.queue_name.clone();
            let notify_task_id = task_id.to_owned();
            tokio::spawn(async move {
                let delay = (next_retry_at - Utc::now()).to_std().unwrap_or_default();
                tokio::time::sleep(delay).await;
                let notify_sql = format!("SELECT pg_notify('task_queue_{}', $1)", queue);
                if let Err(e) = sqlx::query(&notify_sql)
                    .bind(&notify_task_id)
                    .execute(&pool)
                    .await
                {
                    tracing::warn!(
                        task_id = %notify_task_id,
                        queue = %queue,
                        error = %e,
                        "delayed retry NOTIFY failed; worker polling fallback will recover",
                    );
                }
            });
            ScheduleRetryOutcome::Scheduled
        }
        Ok(false) => {
            // Distinguish expired vs reaper-reclaimed (mirrors Python's postcheck).
            if let Some(good_until) = running.good_until {
                if next_retry_at >= good_until {
                    tracing::info!(
                        task_id = %task_id,
                        "retry expired: next_retry_at would exceed good_until",
                    );
                    return ScheduleRetryOutcome::Expired;
                }
            }
            tracing::info!(
                task_id = %task_id,
                "requeue blocked — task no longer RUNNING (reaper likely reclaimed)",
            );
            ScheduleRetryOutcome::ReaperReclaimed
        }
        Err(e) => {
            tracing::error!(
                task_id = %task_id,
                error = %e,
                "failed to requeue task for retry after retries exhausted",
            );
            ScheduleRetryOutcome::DbError
        }
    }
}

// ---------------------------------------------------------------------------
// Finalize Phase 1: Persist terminal state
// ---------------------------------------------------------------------------

/// Phase 1 of finalization: persist the terminal task state and attempt history atomically.
///
/// Returns `Ok(FinalizeOutcome)` on success, `Err(FinalizeError)` on DB failure.
/// The error carries stage information for phase-aware retry.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn persist_terminal_state(
    broker: &PostgresBroker,
    task_id: &str,
    result: TaskResult<Vec<u8>>,
    row: &ClaimedTaskRow,
    running: &SetRunningRow,
    worker_id: &str,
    hostname: &str,
    payload_policy: &PayloadPolicy,
) -> Result<FinalizeOutcome, FinalizeError> {
    let pid = std::process::id() as i32;
    let process_name = format!("worker-{}", pid);
    let task_started_at = running.started_at;
    let attempt_num = running.retry_count + 1;
    let now = Utc::now();
    let worker = WorkerProcessInfo {
        worker_id,
        hostname,
        pid,
        process_name: &process_name,
    };

    match result {
        TaskResult::Ok(ref result_bytes) => {
            // Warn-only: results are never rejected over size (the work is
            // done; destroying it would convert a size concern into data
            // loss). The success payload bytes are measured as produced; the
            // persisted envelope adds only a constant wrapper. Parity with
            // horsies PR #208.
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
                attempt_num,
                task_started_at,
                now,
                worker_id,
                hostname,
                pid,
                &process_name,
                row.is_workflow_task,
                &row.queue_name,
            )
            .await
        }
        TaskResult::Err(ref task_error) => {
            // Check retry eligibility.
            if should_retry(
                task_error,
                running.retry_count,
                running.max_retries,
                row.task_options.as_deref(),
                running.good_until,
            ) {
                match schedule_retry_for_task(
                    broker,
                    task_id,
                    task_error,
                    row,
                    running,
                    attempt_num,
                    now,
                    &worker,
                )
                .await
                {
                    ScheduleRetryOutcome::Scheduled => return Ok(FinalizeOutcome::Retried),
                    ScheduleRetryOutcome::Expired => {
                        tracing::info!(
                            task_id = %task_id,
                            "retry skipped: good_until exceeded, falling through to terminal failure",
                        );
                    }
                    ScheduleRetryOutcome::ReaperReclaimed => {
                        tracing::warn!(
                            task_id = %task_id,
                            "retry aborted: task no longer RUNNING, falling through to terminal failure",
                        );
                    }
                    ScheduleRetryOutcome::DbError => {
                        tracing::error!(
                            task_id = %task_id,
                            "retry DB error, falling through to terminal failure",
                        );
                    }
                }
                // All non-Scheduled outcomes fall through to terminal failure.
            }

            persist_err_terminal(
                broker,
                task_id,
                task_error,
                attempt_num,
                task_started_at,
                now,
                worker_id,
                hostname,
                pid,
                &process_name,
                &row.task_name,
                payload_policy,
            )
            .await
        }
    }
}

/// Persist a successful task result (or serialization fallback) atomically.
async fn persist_ok_result(
    broker: &PostgresBroker,
    task_id: &str,
    result_bytes: &[u8],
    attempt_num: i32,
    task_started_at: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
    worker_id: &str,
    hostname: &str,
    pid: i32,
    process_name: &str,
    is_workflow_task: bool,
    queue_name: &str,
) -> Result<FinalizeOutcome, FinalizeError> {
    // Wrap the task's result bytes via `&RawValue`: parsing validates the JSON
    // exactly like the previous full `Value` parse, but the adjacently-tagged
    // wrapper then embeds the original bytes verbatim instead of building and
    // re-serializing a full `Value` tree — no payload-sized intermediate, and no
    // reformatting of numbers/whitespace on the way to the `result` column (P6).
    let (wrapped_json, is_success) =
        match serde_json::from_slice::<&serde_json::value::RawValue>(result_bytes) {
            Ok(raw) => (
                serde_json::to_string(&TaskResult::Ok(raw)).unwrap_or_else(|_| "{}".to_owned()),
                true,
            ),
            Err(e) => (
                serde_json::to_string(&TaskResult::<serde_json::Value>::Err(TaskError::builtin(
                    OperationalErrorCode::WorkerSerializationError,
                    format!("failed to parse task result JSON: {}", e),
                )))
                .unwrap_or_else(|_| "{}".to_owned()),
                false,
            ),
        };

    // Plain (non-workflow) ok result: one fused statement (lock + attempt + CAS +
    // capacity notify) finalizes it with no Phase 2. Workflow tasks keep the
    // multi-statement path so Phase 2 can run the workflow callback. Parity with
    // horsies PR #134.
    if is_success && !is_workflow_task {
        let channel = format!("task_queue_{}", queue_name);
        let payload = format!("capacity:{}", task_id);
        let tx_result = finalize_with_retry(task_id, "complete-fused", || async {
            broker
                .finalize_completed_fused(
                    task_id,
                    Some(&wrapped_json),
                    worker_id,
                    &channel,
                    &payload,
                    Some(task_started_at),
                )
                .await
        })
        .await;

        return match tx_result {
            Ok(true) => Ok(FinalizeOutcome::Finalized),
            Ok(false) => Err(FinalizeError {
                stage: FinalizeStage::Phase1Persist,
                task_id: task_id.to_owned(),
                message: "finalize (complete-fused) aborted: task no longer RUNNING".to_owned(),
                retryable: false,
            }),
            Err(e) => Err(FinalizeError {
                stage: FinalizeStage::Phase1Persist,
                task_id: task_id.to_owned(),
                message: format!("finalize (complete-fused) failed after retries: {}", e),
                retryable: e.is_retryable(),
            }),
        };
    }

    if is_success {
        let tx_result = finalize_with_retry(task_id, "complete", || async {
            let mut tx = broker
                .pool()
                .begin()
                .await
                .map_err(crate::broker::BrokerError::Database)?;
            let applied = broker
                .complete_in_tx(
                    &mut tx,
                    task_id,
                    Some(&wrapped_json),
                    worker_id,
                    Some(task_started_at),
                )
                .await?;
            if applied {
                broker
                    .upsert_task_attempt(
                        &mut tx,
                        task_id,
                        attempt_num,
                        "COMPLETED",
                        false,
                        task_started_at,
                        now,
                        None,
                        None,
                        None,
                        Some(worker_id),
                        Some(hostname),
                        Some(pid),
                        Some(process_name),
                    )
                    .await?;
            }
            tx.commit()
                .await
                .map_err(crate::broker::BrokerError::Database)?;
            Ok::<bool, crate::broker::BrokerError>(applied)
        })
        .await;

        match tx_result {
            Ok(true) => Ok(FinalizeOutcome::Terminal {
                result_json: wrapped_json,
                is_success: true,
            }),
            Ok(false) => {
                // Task no longer RUNNING (reaper reclaimed). Not retryable.
                Err(FinalizeError {
                    stage: FinalizeStage::Phase1Persist,
                    task_id: task_id.to_owned(),
                    message: "finalize (complete) aborted: task no longer RUNNING".to_owned(),
                    retryable: false,
                })
            }
            Err(e) => Err(FinalizeError {
                stage: FinalizeStage::Phase1Persist,
                task_id: task_id.to_owned(),
                message: format!("finalize (complete) failed after retries: {}", e),
                retryable: e.is_retryable(),
            }),
        }
    } else {
        let ser_error_code = OperationalErrorCode::WorkerSerializationError.to_string();
        let tx_result = finalize_with_retry(task_id, "fail/serialization", || async {
            let mut tx = broker
                .pool()
                .begin()
                .await
                .map_err(crate::broker::BrokerError::Database)?;
            let applied = broker
                .fail_in_tx(
                    &mut tx,
                    task_id,
                    Some(&wrapped_json),
                    Some(&ser_error_code),
                    worker_id,
                    Some(task_started_at),
                )
                .await?;
            if applied {
                broker
                    .upsert_task_attempt(
                        &mut tx,
                        task_id,
                        attempt_num,
                        "FAILED",
                        false,
                        task_started_at,
                        now,
                        Some(&ser_error_code),
                        Some("serialization error"),
                        None,
                        Some(worker_id),
                        Some(hostname),
                        Some(pid),
                        Some(process_name),
                    )
                    .await?;
            }
            tx.commit()
                .await
                .map_err(crate::broker::BrokerError::Database)?;
            Ok::<bool, crate::broker::BrokerError>(applied)
        })
        .await;

        match tx_result {
            Ok(true) => Ok(FinalizeOutcome::Terminal {
                result_json: wrapped_json,
                is_success: false,
            }),
            Ok(false) => Err(FinalizeError {
                stage: FinalizeStage::Phase1Persist,
                task_id: task_id.to_owned(),
                message: "finalize (fail/serialization) aborted: task no longer RUNNING".to_owned(),
                retryable: false,
            }),
            Err(e) => Err(FinalizeError {
                stage: FinalizeStage::Phase1Persist,
                task_id: task_id.to_owned(),
                message: format!("finalize (fail/serialization) failed after retries: {}", e),
                retryable: e.is_retryable(),
            }),
        }
    }
}

/// Persist a terminal task failure atomically.
#[allow(clippy::too_many_arguments)]
async fn persist_err_terminal(
    broker: &PostgresBroker,
    task_id: &str,
    task_error: &TaskError,
    attempt_num: i32,
    task_started_at: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
    worker_id: &str,
    hostname: &str,
    pid: i32,
    process_name: &str,
    task_name: &str,
    payload_policy: &PayloadPolicy,
) -> Result<FinalizeOutcome, FinalizeError> {
    let wrapped = TaskResult::<serde_json::Value>::Err(task_error.clone());
    let wrapped_json = serde_json::to_string(&wrapped).unwrap_or_else(|_| "{}".to_owned());
    // Warn-only: error envelopes can carry oversized data too; measured on
    // the exact persisted string, already in hand. Parity with horsies PR #208.
    enforce_payload_policy(
        payload_policy,
        task_name,
        PayloadKind::Result,
        wrapped_json.len(),
    );
    let error_code_str = task_error.error_code.as_ref().map(|c| c.to_string());
    let error_msg = task_error.message.as_deref();

    let tx_result = finalize_with_retry(task_id, "fail", || async {
        let mut tx = broker
            .pool()
            .begin()
            .await
            .map_err(crate::broker::BrokerError::Database)?;
        let applied = broker
            .fail_in_tx(
                &mut tx,
                task_id,
                Some(&wrapped_json),
                error_code_str.as_deref(),
                worker_id,
                Some(task_started_at),
            )
            .await?;
        if applied {
            broker
                .upsert_task_attempt(
                    &mut tx,
                    task_id,
                    attempt_num,
                    "FAILED",
                    false,
                    task_started_at,
                    now,
                    error_code_str.as_deref(),
                    error_msg,
                    None,
                    Some(worker_id),
                    Some(hostname),
                    Some(pid),
                    Some(process_name),
                )
                .await?;
        }
        tx.commit()
            .await
            .map_err(crate::broker::BrokerError::Database)?;
        Ok::<bool, crate::broker::BrokerError>(applied)
    })
    .await;

    match tx_result {
        Ok(true) => Ok(FinalizeOutcome::Terminal {
            result_json: wrapped_json,
            is_success: false,
        }),
        Ok(false) => Err(FinalizeError {
            stage: FinalizeStage::Phase1Persist,
            task_id: task_id.to_owned(),
            message: "finalize (fail) aborted: task no longer RUNNING".to_owned(),
            retryable: false,
        }),
        Err(e) => Err(FinalizeError {
            stage: FinalizeStage::Phase1Persist,
            task_id: task_id.to_owned(),
            message: format!("finalize (fail) failed after retries: {}", e),
            retryable: e.is_retryable(),
        }),
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
    task_id: &str,
    result_json: &str,
    is_success: bool,
    queue_name: &str,
    is_workflow_task: bool,
) -> Result<(), FinalizeError> {
    // Workflow membership is carried on the task row (is_workflow_task), set at
    // insert time — no per-task JOIN to horsies_workflow_tasks needed here.
    if is_workflow_task {
        crate::workflow_engine::on_workflow_task_complete(
            pool,
            task_id,
            result_json,
            is_success,
            workflow_registry,
        )
        .await
        .map_err(|e| {
            let retryable = is_retryable_workflow_error(&e);
            FinalizeError {
                stage: FinalizeStage::Phase2Workflow,
                task_id: task_id.to_owned(),
                message: format!("workflow callback failed: {}", e),
                retryable,
            }
        })?;
    }

    // Capacity notification — non-fatal if it fails.
    notify_worker_capacity(pool, queue_name, task_id).await;

    Ok(())
}

/// Load a terminal task's persisted result for Phase 2 replay.
///
/// When Phase 1 succeeded but Phase 2 failed, this reloads the already-persisted
/// TaskResult so Phase 2 can be re-run without re-executing the task.
async fn load_persisted_task_result(
    pool: &sqlx::PgPool,
    task_id: &str,
) -> Result<(String, bool), FinalizeError> {
    // EXPIRED is included: an expired-before-start workflow task produces
    // Phase 2 work (is_success = false) and must be reloadable for in-process
    // replay, matching the terminal statuses recovery Case 1.7 treats as
    // terminal. CANCELLED is intentionally omitted: a cancelled task is not
    // finalized through the Phase 1 → Phase 2 path, so it never reaches this
    // reload (the Ok(None) ownership branch handles PAUSED/CANCELLED workflows
    // without producing a replayable result).
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT status, result FROM horsies_tasks WHERE id = $1 AND status IN ('COMPLETED', 'FAILED', 'EXPIRED')",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| FinalizeError {
        stage: FinalizeStage::Phase2Workflow,
        task_id: task_id.to_owned(),
        message: format!("failed to load persisted task result: {}", e),
        retryable: true,
    })?;

    match row {
        Some((status, Some(result_json))) => {
            let is_success = status == "COMPLETED";
            Ok((result_json, is_success))
        }
        _ => Err(FinalizeError {
            stage: FinalizeStage::Phase2Workflow,
            task_id: task_id.to_owned(),
            message: "cannot replay Phase 2: terminal task result unavailable".to_owned(),
            retryable: false,
        }),
    }
}

pub(crate) async fn notify_worker_capacity(pool: &sqlx::PgPool, queue_name: &str, task_id: &str) {
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
) -> Option<Phase2Work> {
    let task_id = row.id.clone();
    let queue_name = row.queue_name.clone();
    let is_workflow_task = row.is_workflow_task;
    let pid = std::process::id() as i32;

    let running = match confirm_ownership_and_set_running(
        &broker,
        &task_id,
        &worker_id,
        pid,
        &hostname,
        is_workflow_task,
        row.claimed_at,
    )
    .await
    {
        OwnershipOutcome::Running(running) => running,
        OwnershipOutcome::ExpiredBeforeStart { result_json } => {
            return Some(Phase2Work {
                task_id,
                result_json,
                is_success: false,
                queue_name,
                is_workflow_task,
            });
        }
        OwnershipOutcome::Aborted => return None,
    };

    match retry_phase1(
        &broker,
        &task_id,
        TaskResult::Err(task_error),
        &row,
        &running,
        &worker_id,
        &hostname,
        &payload_policy,
    )
    .await
    {
        Some(FinalizeOutcome::Terminal {
            result_json,
            is_success,
        }) => Some(Phase2Work {
            task_id,
            result_json,
            is_success,
            queue_name,
            is_workflow_task,
        }),
        Some(FinalizeOutcome::Retried) | Some(FinalizeOutcome::Finalized) | None => None,
    }
}

// ---------------------------------------------------------------------------
// Orchestrator: execute_and_finalize (mirrors Python's _finalize_after)
// ---------------------------------------------------------------------------

/// Execute a task through Phase 1 finalize, returning Phase 2 work if needed.
///
/// Orchestrates:
/// 1. Confirm ownership (CLAIMED → RUNNING)
/// 2. Start heartbeat → build envelope → execute task → stop heartbeat
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
    let task_id = row.id.clone();
    let queue_name = row.queue_name.clone();
    let is_workflow_task = row.is_workflow_task;
    let pid = std::process::id() as i32;
    let accepts_workflow_ctx = task_fn.accepts_workflow_ctx();

    // Phase 0: Confirm ownership and transition to RUNNING.
    let running = match confirm_ownership_and_set_running(
        &broker,
        &task_id,
        &worker_id,
        pid,
        &hostname,
        is_workflow_task,
        row.claimed_at,
    )
    .await
    {
        OwnershipOutcome::Running(running) => running,
        OwnershipOutcome::ExpiredBeforeStart { result_json } => {
            return Some(Phase2Work {
                task_id,
                result_json,
                is_success: false,
                queue_name,
                is_workflow_task,
            });
        }
        OwnershipOutcome::Aborted => return None,
    };

    // Start runner heartbeat.
    let hb_cancel = CancellationToken::new();
    let hb_handle = spawn_runner_heartbeat(
        broker.pool().clone(),
        task_id.clone(),
        worker_id.clone(),
        hostname.clone(),
        pid,
        Duration::from_millis(recovery.runner_heartbeat_interval_ms),
        hb_cancel.clone(),
    );

    // Phase 1: Build envelope + execute task. The deadline (if any) is measured
    // around user-code execution; on expiry the result is TASK_TIMEOUT and the
    // finalize path below decides fail-vs-retry via auto_retry_for.
    let timeout = parse_timeout_ms(row.task_options.as_deref())
        .map(|ms| Duration::from_millis(u64::from(ms)));
    let result = match build_task_envelope(&row, accepts_workflow_ctx) {
        Ok(envelope) => execute_task(task_fn, envelope, timeout).await,
        Err(err) => TaskResult::Err(err),
    };

    // Stop heartbeat.
    hb_cancel.cancel();
    let _ = hb_handle.await;

    // Stamp finalization handoff so the stale-RUNNING reaper skips this row
    // while finalize runs (the runner heartbeat has now stopped). Best-effort:
    // a failed stamp does not abort finalize — phase-1 CAS still protects
    // correctness. Mirrors Python's finalizing_at / finalizing_by_worker_id.
    if let Err(e) = sqlx::query(
        "UPDATE horsies_tasks SET finalizing_at = NOW(), finalizing_by_worker_id = $2 \
         WHERE id = $1 AND status = 'RUNNING'",
    )
    .bind(&task_id)
    .bind(&worker_id)
    .execute(broker.pool())
    .await
    {
        tracing::warn!(task_id = %task_id, error = %e, "failed to stamp finalizing handoff");
    }

    // Finalize Phase 1: Persist terminal state (with phase-aware retry).
    let phase1_outcome = retry_phase1(
        &broker,
        &task_id,
        result,
        &row,
        &running,
        &worker_id,
        &hostname,
        &payload_policy,
    )
    .await;

    match phase1_outcome {
        Some(FinalizeOutcome::Terminal {
            result_json,
            is_success,
        }) => Some(Phase2Work {
            task_id,
            result_json,
            is_success,
            queue_name,
            is_workflow_task,
        }),
        Some(FinalizeOutcome::Retried) | Some(FinalizeOutcome::Finalized) | None => None,
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
) {
    retry_phase2(
        pool,
        workflow_registry,
        &work.task_id,
        &work.result_json,
        work.is_success,
        &work.queue_name,
        work.is_workflow_task,
    )
    .await;
}

/// Run Phase 1 with bounded retries.
///
/// Returns `Some(outcome)` on success, `None` if retries exhausted or non-retryable.
#[allow(clippy::too_many_arguments)]
async fn retry_phase1(
    broker: &PostgresBroker,
    task_id: &str,
    result: TaskResult<Vec<u8>>,
    row: &ClaimedTaskRow,
    running: &SetRunningRow,
    worker_id: &str,
    hostname: &str,
    payload_policy: &PayloadPolicy,
) -> Option<FinalizeOutcome> {
    // First attempt.
    match persist_terminal_state(
        broker,
        task_id,
        result.clone(),
        row,
        running,
        worker_id,
        hostname,
        payload_policy,
    )
    .await
    {
        Ok(outcome) => return Some(outcome),
        Err(e) if !e.retryable => {
            tracing::error!(
                task_id = %task_id,
                error = %e,
                "Phase 1 finalize failed (non-retryable) — task stays RUNNING for reaper",
            );
            return None;
        }
        Err(e) => {
            tracing::warn!(
                task_id = %task_id,
                error = %e,
                "Phase 1 finalize failed, scheduling retries",
            );
        }
    }

    // Retry attempts.
    for attempt in 1..PHASE1_MAX_RETRIES {
        let delay = phase_retry_delay(attempt);
        tracing::warn!(
            task_id = %task_id,
            attempt,
            max = PHASE1_MAX_RETRIES,
            delay_s = format!("{:.1}", delay),
            "retrying Phase 1 finalize",
        );
        tokio::time::sleep(Duration::from_secs_f64(delay)).await;

        match persist_terminal_state(
            broker,
            task_id,
            result.clone(),
            row,
            running,
            worker_id,
            hostname,
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
                    "Phase 1 finalize retry failed (non-retryable)",
                );
                return None;
            }
            Err(e) => {
                tracing::warn!(
                    task_id = %task_id,
                    error = %e,
                    attempt,
                    "Phase 1 finalize retry failed (retryable)",
                );
            }
        }
    }

    tracing::error!(
        task_id = %task_id,
        attempts = PHASE1_MAX_RETRIES,
        "Phase 1 finalize retries exhausted — task stays RUNNING for reaper",
    );
    None
}

/// Run Phase 2 with bounded retries.
///
/// If the initial call fails, reloads the persisted task result from DB
/// (since Phase 1 already committed) and replays.
async fn retry_phase2(
    pool: &sqlx::PgPool,
    workflow_registry: &WorkflowSpecRegistry,
    task_id: &str,
    result_json: &str,
    is_success: bool,
    queue_name: &str,
    is_workflow_task: bool,
) {
    // First attempt with the in-memory result.
    match finalize_workflow_phase(
        pool,
        workflow_registry,
        task_id,
        result_json,
        is_success,
        queue_name,
        is_workflow_task,
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

    // Retry attempts — reload persisted result from DB for each replay.
    for attempt in 1..PHASE2_MAX_RETRIES {
        let delay = phase_retry_delay(attempt);
        tracing::warn!(
            task_id = %task_id,
            attempt,
            max = PHASE2_MAX_RETRIES,
            delay_s = format!("{:.1}", delay),
            "retrying Phase 2 finalize",
        );
        tokio::time::sleep(Duration::from_secs_f64(delay)).await;

        // Reload from DB — Phase 1 already committed the terminal state.
        let (reloaded_json, reloaded_success) =
            match load_persisted_task_result(pool, task_id).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(
                        task_id = %task_id,
                        error = %e,
                        attempt,
                        "Phase 2 replay: failed to reload persisted result",
                    );
                    if !e.retryable {
                        return;
                    }
                    continue;
                }
            };

        match finalize_workflow_phase(
            pool,
            workflow_registry,
            task_id,
            &reloaded_json,
            reloaded_success,
            queue_name,
            is_workflow_task,
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
fn phase_retry_delay(attempt: u32) -> f64 {
    (PHASE_RETRY_BASE_DELAY_S * 2.0f64.powi(attempt as i32 - 1)).min(PHASE_RETRY_MAX_DELAY_S)
}

// ---------------------------------------------------------------------------
// Workflow context parsing (private helper)
// ---------------------------------------------------------------------------

mod parse {
    use std::collections::HashMap;

    use crate::core::task::error::{OperationalErrorCode, TaskError};
    use crate::core::task::result::TaskResult;
    use crate::core::workflow::context::WorkflowContext;
    use crate::core::workflow::SubWorkflowSummary;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct WorkflowCtxPayload {
        workflow_id: String,
        task_index: i32,
        task_name: String,
        #[serde(default)]
        results_by_id: HashMap<String, String>,
        #[serde(default)]
        summaries_by_id: HashMap<String, String>,
    }

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
        for (node_id, result_json) in payload.results_by_id {
            let parsed: TaskResult<serde_json::Value> = serde_json::from_str(&result_json)
                .map_err(|e| {
                    TaskError::builtin(
                        OperationalErrorCode::WorkerSerializationError,
                        format!(
                            "failed to parse workflow context result for node_id '{}': {}",
                            node_id, e,
                        ),
                    )
                })?;
            results_by_id.insert(node_id, parsed);
        }

        let mut summaries_by_id: HashMap<String, SubWorkflowSummary> =
            HashMap::with_capacity(payload.summaries_by_id.len());
        for (node_id, summary_json) in payload.summaries_by_id {
            let parsed: SubWorkflowSummary = serde_json::from_str(&summary_json).map_err(|e| {
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
            r#"{"__type":"ok","value":{"amount": 1.2300, "id": 7}}"#,
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
mod envelope_tests {
    use super::build_envelope_from_parts;

    /// Parse the envelope bytes back into a Value for shape assertions.
    fn envelope(args: Option<&str>, kwargs: Option<&str>) -> serde_json::Value {
        let bytes = build_envelope_from_parts(args, kwargs, false).expect("envelope built");
        serde_json::from_slice(&bytes).expect("envelope is JSON")
    }

    #[test]
    fn null_args_and_kwargs_yield_empty_envelope() {
        assert_eq!(
            envelope(None, None),
            serde_json::json!({"args": [], "kwargs": {}}),
        );
    }

    #[test]
    fn array_args_passed_through_object_kwargs_passed_through() {
        assert_eq!(
            envelope(Some("[1, 2]"), Some(r#"{"a": 1}"#)),
            serde_json::json!({"args": [1, 2], "kwargs": {"a": 1}}),
        );
    }

    #[test]
    fn scalar_args_coerced_to_single_element_array() {
        assert_eq!(
            envelope(Some("5"), None),
            serde_json::json!({"args": [5], "kwargs": {}}),
        );
    }

    /// The reviewer-flagged divergence: a non-object kwargs is rejected here,
    /// where `decode_task_input` alone would silently fall through to the args
    /// branch. Routing check-time validation through this helper makes the
    /// dry-run reject the same malformed payloads execution rejects.
    #[test]
    fn non_object_kwargs_rejected() {
        let err = build_envelope_from_parts(None, Some(r#""bad""#), false).unwrap_err();
        assert!(err
            .message
            .unwrap_or_default()
            .contains("kwargs payload is not a JSON object"));
    }
}

#[cfg(test)]
mod replay_reload_tests {
    use super::load_persisted_task_result;
    use serial_test::serial;
    use sqlx::PgPool;
    use uuid::Uuid;

    fn test_db_url() -> String {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return url;
        }
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let root = std::path::Path::new(manifest_dir)
            .ancestors()
            .find(|p| p.join(".env").exists());
        if let Some(root) = root {
            if let Ok(contents) = std::fs::read_to_string(root.join(".env")) {
                for line in contents.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    if let Some((key, value)) = line.split_once('=') {
                        if key.trim() == "DB_PASSWORD" {
                            return format!(
                                "postgresql://postgres:{}@localhost:5432/horsies-rust-port",
                                value.trim(),
                            );
                        }
                    }
                }
            }
        }
        panic!("database URL not found: set DATABASE_URL or add DB_PASSWORD to .env");
    }

    async fn test_pool() -> PgPool {
        let pool = PgPool::connect(&test_db_url()).await.expect("connect");
        crate::broker::migrations::run_horsies_migrations(&pool)
            .await
            .expect("migrations");
        pool
    }

    async fn insert_terminal_task(pool: &PgPool, task_id: &str, status: &str, result: Option<&str>) {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status, result,
                sent_at, created_at, updated_at, retry_count, max_retries, enqueue_sha
            ) VALUES (
                $1, 'replay_reload_task', 'default', 100, '[]', '{}', $2, $3,
                NOW(), NOW(), NOW(), 0, 0, $1
            )",
        )
        .bind(task_id)
        .bind(status)
        .bind(result)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn delete_task(pool: &PgPool, task_id: &str) {
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .unwrap();
    }

    /// An expired-before-start task is terminal WITH a result (TASK_EXPIRED
    /// err); Phase 2 replay must reload it. Regression pin for the accepted
    /// status set (parity with horsies PR #165, where EXPIRED rows were
    /// rejected as "terminal task result unavailable").
    #[tokio::test]
    #[serial]
    async fn expired_err_result_reloads_for_phase2_replay() {
        let pool = test_pool().await;
        let task_id = Uuid::new_v4().to_string();
        let stored = r#"{"ok":null,"err":{"error_code":"TASK_EXPIRED","message":"expired before start"}}"#;
        insert_terminal_task(&pool, &task_id, "EXPIRED", Some(stored)).await;

        let (result_json, is_success) = load_persisted_task_result(&pool, &task_id)
            .await
            .expect("EXPIRED row with a stored result must be replayable");
        assert!(!is_success, "EXPIRED replays as a failure result");
        assert_eq!(result_json, stored);

        delete_task(&pool, &task_id).await;
    }

    /// CANCELLED is deliberately outside the replay set: it is the
    /// result-less terminal state and workflows advance through the cancel
    /// cascade, not result replay.
    #[tokio::test]
    #[serial]
    async fn cancelled_row_is_not_replayable() {
        let pool = test_pool().await;
        let task_id = Uuid::new_v4().to_string();
        insert_terminal_task(&pool, &task_id, "CANCELLED", None).await;

        let err = load_persisted_task_result(&pool, &task_id)
            .await
            .expect_err("CANCELLED must not produce a replayable result");
        assert!(!err.retryable, "missing terminal result is permanent");

        delete_task(&pool, &task_id).await;
    }
}

#[cfg(test)]
mod set_running_gate_tests {
    //! P1: `confirm_ownership_and_set_running` gates the workflow_task RUNNING
    //! UPDATE on `is_workflow_task` to drop a per-task-start round trip for plain
    //! tasks. Cross-check: the update must STILL happen for workflow tasks.
    use super::{confirm_ownership_and_set_running, OwnershipOutcome};
    use crate::broker::PostgresBroker;
    use serial_test::serial;
    use sqlx::PgPool;
    use uuid::Uuid;

    fn test_db_url() -> String {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return url;
        }
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let root = std::path::Path::new(manifest_dir)
            .ancestors()
            .find(|p| p.join(".env").exists());
        let pw = root
            .and_then(|r| std::fs::read_to_string(r.join(".env")).ok())
            .and_then(|c| {
                c.lines()
                    .filter_map(|l| l.trim().split_once('='))
                    .find(|(k, _)| k.trim() == "DB_PASSWORD")
                    .map(|(_, v)| v.trim().to_owned())
            })
            .unwrap_or_else(|| "W0rklane".to_owned());
        format!("postgresql://postgres:{pw}@localhost:5432/horsies-rust-port")
    }

    async fn seed_claimed(pool: &PgPool, task_id: &str, is_wf: bool) {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, claimed_at, created_at, updated_at, claimed,
                claimed_by_worker_id, retry_count, max_retries, is_workflow_task, enqueue_sha
            ) VALUES ($1, 'p1_task', 'default', 100, '[]', '{}', 'CLAIMED',
                      NOW(), NOW(), NOW(), NOW(), TRUE, 'w1', 0, 3, $2, $1)",
        )
        .bind(task_id)
        .bind(is_wf)
        .execute(pool)
        .await
        .expect("seed task");
    }

    #[tokio::test]
    #[serial]
    async fn workflow_task_still_transitions_node_to_running() {
        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
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
        .bind(&wf_id)
        .execute(&pool)
        .await
        .expect("insert workflow");
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, task_id, created_at
            ) VALUES ($1, $2, 0, 'node_0', 'p1_task', '[]', '{}',
                      'default', 100, '{}', FALSE, 'all', 'ENQUEUED', FALSE, $3, NOW())",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&wf_id)
        .bind(&task_id)
        .execute(&pool)
        .await
        .expect("insert node");
        seed_claimed(&pool, &task_id, true).await;

        let outcome =
            confirm_ownership_and_set_running(&broker, &task_id, "w1", 1, "h1", true, None).await;
        assert!(matches!(outcome, OwnershipOutcome::Running(_)));

        let node_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1")
                .bind(&wf_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(node_status, "RUNNING", "workflow node must transition to RUNNING (gate must not skip it)");

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1").bind(&wf_id).execute(&pool).await.ok();
        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1").bind(&task_id).execute(&pool).await.ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1").bind(&wf_id).execute(&pool).await.ok();
    }

    #[tokio::test]
    #[serial]
    async fn plain_task_starts_running_without_workflow_update() {
        let broker = PostgresBroker::connect(&test_db_url()).await.expect("connect");
        let pool = broker.pool().clone();
        let task_id = Uuid::new_v4().to_string();
        seed_claimed(&pool, &task_id, false).await;

        let outcome =
            confirm_ownership_and_set_running(&broker, &task_id, "w1", 1, "h1", false, None).await;
        assert!(matches!(outcome, OwnershipOutcome::Running(_)));

        let status: String = sqlx::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
            .bind(&task_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "RUNNING");

        sqlx::query("DELETE FROM horsies_tasks WHERE id = $1").bind(&task_id).execute(&pool).await.ok();
    }
}

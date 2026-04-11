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

use crate::broker::{ClaimedTaskRow, PostgresBroker};
use crate::core::config::recovery::RecoveryConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::task::error::{OperationalErrorCode, TaskError};
use crate::core::task::fn_trait::RegisteredTask;
use crate::core::task::result::TaskResult;
use crate::core::workflow::context::WORKFLOW_CTX_KWARG;

use crate::worker::heartbeat::spawn_runner_heartbeat;
use crate::worker::retry::{calculate_retry_delay, should_retry};

// Re-export for worker.rs orchestrator.
pub(crate) use self::parse::parse_workflow_ctx;

// ---------------------------------------------------------------------------
// Phase types
// ---------------------------------------------------------------------------

/// Outcome of confirming task ownership and transitioning to RUNNING.
pub(crate) enum OwnershipOutcome {
    /// Task transitioned to RUNNING at the given timestamp.
    Running(chrono::DateTime<Utc>),
    /// Task transitioned to RUNNING, but pre-execution bookkeeping failed.
    ///
    /// The task should fail closed without executing user code.
    PreExecutionFailure {
        started_at: chrono::DateTime<Utc>,
        task_error: TaskError,
    },
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
pub(crate) async fn confirm_ownership_and_set_running(
    broker: &PostgresBroker,
    task_id: &str,
    worker_id: &str,
    pid: i32,
    hostname: &str,
) -> OwnershipOutcome {
    match broker
        .set_running(task_id, worker_id, pid, hostname, "worker")
        .await
    {
        Ok(Some(started_at)) => {
            if let Err(e) = sqlx::query(
                "UPDATE horsies_workflow_tasks \
                 SET status = 'RUNNING' \
                 WHERE task_id = $1 AND status = 'ENQUEUED'",
            )
            .bind(task_id)
            .execute(broker.pool())
            .await
            {
                let task_error = TaskError::builtin(
                    OperationalErrorCode::BrokerError,
                    format!("failed to update workflow task to RUNNING: {}", e),
                );
                tracing::error!(task_id = %task_id, error = %e, "failed to update workflow task to RUNNING");
                return OwnershipOutcome::PreExecutionFailure {
                    started_at,
                    task_error,
                };
            }
            OwnershipOutcome::Running(started_at)
        }
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
            if let Err(ue) =
                unclaim_task_with_retry(broker, task_id, worker_id, "set RUNNING failed").await
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
    context: &str,
) -> Result<bool, crate::broker::BrokerError> {
    let mut last_err: Option<crate::broker::BrokerError> = None;

    for attempt in 1..=PRESTART_DB_RETRY_ATTEMPTS {
        match broker.unclaim_task(task_id, worker_id).await {
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
    let args_value: serde_json::Value = match row.args.as_deref() {
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

    let kwargs_value: serde_json::Value = match row.kwargs.as_deref() {
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
pub(crate) async fn execute_task(
    task_fn: RegisteredTask,
    envelope: Vec<u8>,
) -> TaskResult<Vec<u8>> {
    match task_fn {
        RegisteredTask::Async { task: f, .. } => {
            let handle = tokio::spawn(async move { f.execute(&envelope).await });
            match handle.await {
                Ok(r) => r,
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
            match handle.await {
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
    attempt_num: i32,
    task_started_at: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
    worker: &WorkerProcessInfo<'_>,
) -> ScheduleRetryOutcome {
    let new_count = row.retry_count + 1;
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
            .requeue_in_tx(&mut tx, task_id, new_count, Some(next_retry_at))
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
            if let Some(good_until) = row.good_until {
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
pub(crate) async fn persist_terminal_state(
    broker: &PostgresBroker,
    task_id: &str,
    result: TaskResult<Vec<u8>>,
    row: &ClaimedTaskRow,
    task_started_at: chrono::DateTime<Utc>,
    worker_id: &str,
    hostname: &str,
) -> Result<FinalizeOutcome, FinalizeError> {
    let pid = std::process::id() as i32;
    let process_name = format!("worker-{}", pid);
    let attempt_num = row.retry_count + 1;
    let now = Utc::now();
    let worker = WorkerProcessInfo {
        worker_id,
        hostname,
        pid,
        process_name: &process_name,
    };

    match result {
        TaskResult::Ok(ref result_bytes) => {
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
            )
            .await
        }
        TaskResult::Err(ref task_error) => {
            // Check retry eligibility.
            if should_retry(
                task_error,
                row.retry_count,
                row.max_retries,
                row.task_options.as_deref(),
                row.good_until,
            ) {
                match schedule_retry_for_task(
                    broker,
                    task_id,
                    task_error,
                    row,
                    attempt_num,
                    task_started_at,
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
) -> Result<FinalizeOutcome, FinalizeError> {
    let (wrapped, is_success) = match serde_json::from_slice::<serde_json::Value>(result_bytes) {
        Ok(v) => (TaskResult::Ok(v), true),
        Err(e) => (
            TaskResult::Err(TaskError::builtin(
                OperationalErrorCode::WorkerSerializationError,
                format!("failed to parse task result JSON: {}", e),
            )),
            false,
        ),
    };

    let wrapped_json = serde_json::to_string(&wrapped).unwrap_or_else(|_| "{}".to_owned());

    if is_success {
        let tx_result = finalize_with_retry(task_id, "complete", || async {
            let mut tx = broker
                .pool()
                .begin()
                .await
                .map_err(crate::broker::BrokerError::Database)?;
            let applied = broker
                .complete_in_tx(&mut tx, task_id, Some(&wrapped_json))
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
                .fail_in_tx(&mut tx, task_id, Some(&wrapped_json), Some(&ser_error_code))
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
) -> Result<FinalizeOutcome, FinalizeError> {
    let wrapped = TaskResult::<serde_json::Value>::Err(task_error.clone());
    let wrapped_json = serde_json::to_string(&wrapped).unwrap_or_else(|_| "{}".to_owned());
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
) -> Result<(), FinalizeError> {
    // Check if task is part of a workflow.
    let is_wf_task: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM horsies_workflow_tasks WHERE task_id = $1)",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .unwrap_or(false);

    if is_wf_task {
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
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT status, result FROM horsies_tasks WHERE id = $1 AND status IN ('COMPLETED', 'FAILED')",
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
    let payload = format!("capacity:{}", task_id);
    if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
        .bind("task_new")
        .bind(&payload)
        .execute(pool)
        .await
    {
        tracing::warn!(
            task_id = %task_id,
            channel = "task_new",
            error = %e,
            "capacity NOTIFY failed; worker polling fallback will recover",
        );
    }

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
) -> Option<Phase2Work> {
    let task_id = row.id.clone();
    let queue_name = row.queue_name.clone();
    let pid = std::process::id() as i32;

    let task_started_at = match confirm_ownership_and_set_running(
        &broker, &task_id, &worker_id, pid, &hostname,
    )
    .await
    {
        OwnershipOutcome::Running(started_at) => started_at,
        OwnershipOutcome::PreExecutionFailure {
            started_at,
            task_error,
        } => {
            return match retry_phase1(
                &broker,
                &task_id,
                TaskResult::Err(task_error),
                &row,
                started_at,
                &worker_id,
                &hostname,
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
                }),
                Some(FinalizeOutcome::Retried) | None => None,
            };
        }
        OwnershipOutcome::ExpiredBeforeStart { result_json } => {
            return Some(Phase2Work {
                task_id,
                result_json,
                is_success: false,
                queue_name,
            });
        }
        OwnershipOutcome::Aborted => return None,
    };

    match retry_phase1(
        &broker,
        &task_id,
        TaskResult::Err(task_error),
        &row,
        task_started_at,
        &worker_id,
        &hostname,
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
        }),
        Some(FinalizeOutcome::Retried) | None => None,
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
) -> Option<Phase2Work> {
    let task_id = row.id.clone();
    let queue_name = row.queue_name.clone();
    let pid = std::process::id() as i32;
    let accepts_workflow_ctx = task_fn.accepts_workflow_ctx();

    // Phase 0: Confirm ownership and transition to RUNNING.
    let task_started_at = match confirm_ownership_and_set_running(
        &broker, &task_id, &worker_id, pid, &hostname,
    )
    .await
    {
        OwnershipOutcome::Running(started_at) => started_at,
        OwnershipOutcome::PreExecutionFailure {
            started_at,
            task_error,
        } => {
            return match retry_phase1(
                &broker,
                &task_id,
                TaskResult::Err(task_error),
                &row,
                started_at,
                &worker_id,
                &hostname,
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
                }),
                Some(FinalizeOutcome::Retried) | None => None,
            };
        }
        OwnershipOutcome::ExpiredBeforeStart { result_json } => {
            return Some(Phase2Work {
                task_id,
                result_json,
                is_success: false,
                queue_name,
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

    // Phase 1: Build envelope + execute task.
    let result = match build_task_envelope(&row, accepts_workflow_ctx) {
        Ok(envelope) => execute_task(task_fn, envelope).await,
        Err(err) => TaskResult::Err(err),
    };

    // Stop heartbeat.
    hb_cancel.cancel();
    let _ = hb_handle.await;

    // Finalize Phase 1: Persist terminal state (with phase-aware retry).
    let phase1_outcome = retry_phase1(
        &broker,
        &task_id,
        result,
        &row,
        task_started_at,
        &worker_id,
        &hostname,
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
        }),
        Some(FinalizeOutcome::Retried) | None => None,
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
    )
    .await;
}

/// Run Phase 1 with bounded retries.
///
/// Returns `Some(outcome)` on success, `None` if retries exhausted or non-retryable.
async fn retry_phase1(
    broker: &PostgresBroker,
    task_id: &str,
    result: TaskResult<Vec<u8>>,
    row: &ClaimedTaskRow,
    task_started_at: chrono::DateTime<Utc>,
    worker_id: &str,
    hostname: &str,
) -> Option<FinalizeOutcome> {
    // First attempt.
    match persist_terminal_state(
        broker,
        task_id,
        result.clone(),
        row,
        task_started_at,
        worker_id,
        hostname,
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
            task_started_at,
            worker_id,
            hostname,
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
) {
    // First attempt with the in-memory result.
    match finalize_workflow_phase(
        pool,
        workflow_registry,
        task_id,
        result_json,
        is_success,
        queue_name,
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

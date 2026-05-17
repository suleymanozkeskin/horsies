use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::broker::{ClaimedTaskRow, NotifyListener, PostgresBroker};
use crate::core::config::app::AppConfig;
use crate::core::registry::task::TaskRegistry;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::task::error::{OperationalErrorCode, TaskError};

use crate::worker::backoff::RetryBackoff;
use crate::worker::config::WorkerConfig;
use crate::worker::error::WorkerError;
use crate::worker::execution;
use crate::worker::heartbeat::spawn_claimer_heartbeat;
use crate::worker::recovery::{spawn_reaper, spawn_workflow_recovery};

/// Task queue worker.
///
/// Claims tasks from PostgreSQL, executes them according to their
/// registered function (async or blocking), and writes results back.
pub struct Worker {
    broker: Arc<PostgresBroker>,
    registry: Arc<TaskRegistry>,
    workflow_registry: Arc<WorkflowSpecRegistry>,
    app_config: AppConfig,
    worker_config: WorkerConfig,
    worker_id: String,
    semaphore: Arc<Semaphore>,
    tracker: TaskTracker,
    cancel: CancellationToken,
    hostname: String,
}

impl Worker {
    /// Create a new worker.
    pub fn new(
        broker: Arc<PostgresBroker>,
        registry: Arc<TaskRegistry>,
        workflow_registry: Arc<WorkflowSpecRegistry>,
        app_config: AppConfig,
        worker_config: WorkerConfig,
    ) -> Result<Self, WorkerError> {
        worker_config.validate().map_err(WorkerError::Config)?;

        let hostname = gethostname::gethostname()
            .into_string()
            .unwrap_or_else(|_| "unknown".to_owned());

        Ok(Self {
            semaphore: Arc::new(Semaphore::new(worker_config.concurrency as usize)),
            broker,
            registry,
            workflow_registry,
            app_config,
            worker_config,
            worker_id: Uuid::new_v4().to_string(),
            tracker: TaskTracker::new(),
            cancel: CancellationToken::new(),
            hostname,
        })
    }

    /// Run the worker with automatic SIGINT/SIGTERM signal handling.
    ///
    /// Convenience wrapper around `run()` that sets up signal handlers
    /// to trigger graceful shutdown on CTRL+C or SIGTERM.
    pub async fn run_with_signals(&self) -> Result<(), WorkerError> {
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            cancel.cancel();
        });

        #[cfg(unix)]
        {
            let cancel = self.cancel.clone();
            tokio::spawn(async move {
                use tokio::signal::unix::{signal, SignalKind};
                match signal(SignalKind::terminate()) {
                    Ok(mut sig) => {
                        sig.recv().await;
                        cancel.cancel();
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to register SIGTERM handler, graceful shutdown via SIGTERM unavailable");
                    }
                }
            });
        }

        self.run().await
    }

    /// Run the worker until cancelled or a fatal error occurs.
    pub async fn run(&self) -> Result<(), WorkerError> {
        // Print the startup banner before any log lines.
        crate::worker::cli::banner::print_banner(&crate::worker::cli::banner::BannerInfo {
            worker_id: &self.worker_id,
            worker_config: &self.worker_config,
            app_config: &self.app_config,
            task_registry: &self.registry,
            role: "worker",
        });

        tracing::info!(
            worker_id = %self.worker_id,
            queues = ?self.worker_config.queues,
            concurrency = self.worker_config.concurrency,
            cluster_wide_cap = ?self.app_config.cluster_wide_cap,
            prefetch_buffer = self.app_config.prefetch_buffer,
            claim_lease_ms = ?self.app_config.claim_lease_ms,
            "worker starting",
        );
        if self.app_config.prefetch_buffer > 0 && self.app_config.claim_lease_ms.is_none() {
            tracing::warn!(
                prefetch_buffer = self.app_config.prefetch_buffer,
                claimed_stale_threshold_ms = self.app_config.recovery.claimed_stale_threshold_ms,
                "prefetch enabled without claim_lease_ms; CLAIMED tasks may be requeued after claimed_stale_threshold_ms even while worker is alive",
            );
        }

        // Subscribe to queue notifications with resilience.
        let mut listener = self.connect_listener_with_resilience().await?;

        // Fan out LISTEN/NOTIFY to an mpsc channel to avoid canceling recv().
        // Use a bounded channel to prevent memory exhaustion under high notification load.
        // Notifications are coalesceable wake-up signals, so dropping overflow is safe.
        let (notify_tx, mut notify_rx) = mpsc::channel(256);
        let listener_cancel = self.cancel.clone();
        let _listener_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = listener_cancel.cancelled() => break,
                    notification = listener.recv() => {
                        match notification {
                            Ok(notif) => {
                                // Use try_send to avoid blocking; drop if buffer full.
                                if notify_tx.try_send(notif).is_err() {
                                    // Channel full or closed - ok to drop, worker will poll anyway.
                                }
                            }
                            Err(e) => {
                                // sqlx PgListener reconnects automatically on
                                // network failures and re-subscribes to channels.
                                // Pause briefly to avoid a tight spin if
                                // reconnection keeps failing.
                                tracing::error!(error = %e, "listener error, sqlx will attempt reconnect");
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            }
                        }
                    }
                }
            }
        });

        // Start claimer heartbeat.
        let claimer_interval =
            Duration::from_millis(self.app_config.recovery.claimer_heartbeat_interval_ms);
        let _claimer_hb = spawn_claimer_heartbeat(
            self.broker.pool().clone(),
            self.worker_id.clone(),
            self.hostname.clone(),
            std::process::id() as i32,
            claimer_interval,
            self.app_config.claim_lease_ms,
            self.app_config.max_claim_renew_age_ms,
            self.cancel.clone(),
        );

        // Start reaper.
        let _reaper = spawn_reaper(
            self.broker.pool().clone(),
            self.app_config.recovery.clone(),
            self.cancel.clone(),
        );

        // Start workflow recovery loop.
        let _wf_recovery = spawn_workflow_recovery(
            self.broker.pool().clone(),
            Arc::clone(&self.workflow_registry),
            self.app_config.recovery.clone(),
            self.cancel.clone(),
        );

        // Start worker state snapshot loop.
        let worker_started_at = Utc::now();
        let _state_loop = crate::worker::worker_state::spawn_worker_state_loop(
            self.broker.pool().clone(),
            self.worker_id.clone(),
            self.hostname.clone(),
            std::process::id() as i32,
            self.worker_config.clone(),
            self.app_config.clone(),
            Arc::clone(&self.semaphore),
            worker_started_at,
            self.cancel.clone(),
        );

        tracing::info!(worker_id = %self.worker_id, "worker ready");

        // Main-loop claim error backoff.
        let mut claim_backoff = RetryBackoff::from_config(&self.app_config.resilience);

        // Main loop.
        loop {
            if self.cancel.is_cancelled() {
                break;
            }

            // Claim and dispatch.
            match self.claim_and_dispatch_all().await {
                Ok(true) => {
                    claim_backoff.reset();
                    continue; // more work might be available
                }
                Ok(false) => {
                    claim_backoff.reset();
                }
                Err(e) => {
                    if e.is_retryable() && claim_backoff.can_retry() {
                        let delay = claim_backoff.next_delay_seconds();
                        tracing::warn!(
                            error = %e,
                            attempt = claim_backoff.attempts(),
                            delay_s = format!("{:.2}", delay),
                            "retryable claim error, backing off",
                        );
                        tokio::select! {
                            _ = self.cancel.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_secs_f64(delay)) => {}
                        }
                        continue;
                    } else if e.is_retryable() {
                        tracing::error!(
                            error = %e,
                            attempts = claim_backoff.attempts(),
                            "retryable claim error, max retries exhausted — shutting down",
                        );
                        break;
                    } else {
                        tracing::error!(
                            error = %e,
                            "non-retryable claim error — shutting down",
                        );
                        break;
                    }
                }
            }

            // Wait for a NOTIFY or timeout (configurable poll interval).
            let poll_interval =
                Duration::from_millis(self.app_config.resilience.notify_poll_interval_ms);
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                result = tokio::time::timeout(
                    poll_interval,
                    notify_rx.recv(),
                ) => {
                    match result {
                        Ok(Some(_)) => {
                            // GAP 5: Drain burst notifications (coalesce_notifies).
                            // After waking up on one notification, drain up to
                            // `coalesce_notifies` buffered messages to prevent
                            // thundering herd from burst inserts.
                            let max_drain = self.worker_config.coalesce_notifies;
                            let mut drained = 0u32;
                            while drained < max_drain {
                                match notify_rx.try_recv() {
                                    Ok(_) => { drained += 1; }
                                    Err(mpsc::error::TryRecvError::Empty) => break,
                                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                                }
                            }
                        }
                        Ok(None) => {
                            tracing::info!("listener channel closed, shutting down");
                            break;
                        }
                        Err(_) => {} // timeout — re-loop to check for work
                    }
                }
            }
        }

        // Graceful shutdown: wait for in-flight tasks.
        self.shutdown().await;
        Ok(())
    }

    /// Request graceful shutdown.
    pub fn request_stop(&self) {
        self.cancel.cancel();
    }

    /// Return a clone of the cancellation token.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Worker instance ID.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    // ----- listener setup with resilience -----

    /// Attempt to connect the LISTEN/NOTIFY listener and subscribe to queues.
    async fn try_connect_listener(&self) -> Result<NotifyListener, WorkerError> {
        if self.broker.pgbouncer_transaction_mode() {
            self.broker.check_listener_delivery().await?;
        }
        let mut listener = NotifyListener::connect(self.broker.session_pool()).await?;
        for queue in &self.worker_config.queues {
            listener.listen(&format!("task_queue_{}", queue)).await?;
        }
        listener.listen("task_new").await?;
        Ok(listener)
    }

    /// Connect the listener with exponential backoff on transient failures.
    async fn connect_listener_with_resilience(&self) -> Result<NotifyListener, WorkerError> {
        let mut backoff = RetryBackoff::from_config(&self.app_config.resilience);

        loop {
            match self.try_connect_listener().await {
                Ok(listener) => return Ok(listener),
                Err(e) => {
                    if !e.is_retryable() {
                        tracing::error!(error = %e, "non-retryable listener connection error");
                        return Err(e);
                    }
                    if !backoff.can_retry() {
                        tracing::error!(
                            error = %e,
                            attempts = backoff.attempts(),
                            "listener connection retries exhausted",
                        );
                        return Err(e);
                    }
                    let delay = backoff.next_delay_seconds();
                    tracing::warn!(
                        error = %e,
                        attempt = backoff.attempts(),
                        delay_s = format!("{:.2}", delay),
                        "listener connection failed, retrying",
                    );
                    tokio::select! {
                        _ = self.cancel.cancelled() => {
                            return Err(WorkerError::Config(
                                "shutdown requested during listener connection".to_owned(),
                            ));
                        }
                        _ = tokio::time::sleep(Duration::from_secs_f64(delay)) => {}
                    }
                }
            }
        }
    }

    // ----- shutdown -----

    /// Wait for all in-flight tasks to finish (with a timeout).
    async fn shutdown(&self) {
        tracing::info!("shutting down, waiting for in-flight tasks");
        self.tracker.close();
        if tokio::time::timeout(Duration::from_secs(30), self.tracker.wait())
            .await
            .is_err()
        {
            tracing::warn!("worker shutdown timed out with in-flight tasks still running");
        }
        tracing::info!("worker stopped");
    }

    // ----- claim & dispatch -----

    /// Claim tasks from all queues and dispatch them.
    /// Returns `true` if any tasks were claimed (or buffered tasks dispatched).
    ///
    /// Supports two modes:
    /// - **Hard cap** (`prefetch_buffer == 0`): budget =
    ///   `concurrency - (RUNNING + CLAIMED)` for this worker.
    /// - **Soft cap / prefetch** (`prefetch_buffer > 0`): budget =
    ///   `concurrency + prefetch_buffer - running_count`. Claims extra tasks
    ///   with a lease; they sit CLAIMED until a semaphore permit opens up.
    ///
    /// When `cluster_wide_cap` is set (hard cap mode only), the budget is
    /// further reduced so the total RUNNING + CLAIMED tasks across ALL
    /// workers does not exceed the cap.
    async fn claim_and_dispatch_all(&self) -> Result<bool, WorkerError> {
        let prefetch = self.app_config.prefetch_buffer;
        let soft_cap_mode = prefetch > 0;

        // ── max_claim_per_worker guard ──
        // Mirrors Python's guard at the top of _claim_and_dispatch_all:
        // prevents over-claiming beyond the configured (or auto-derived) limit.
        let max_claimed = if self.worker_config.max_claim_per_worker > 0 {
            self.worker_config.max_claim_per_worker
        } else if soft_cap_mode {
            self.worker_config.concurrency + prefetch
        } else {
            self.worker_config.concurrency
        };
        let claimed_count = self
            .broker
            .count_claimed_for_worker(&self.worker_id)
            .await? as u32;
        let can_claim_more = claimed_count < max_claimed;

        // ── Dispatch buffered CLAIMED tasks first ──
        // In prefetch mode (and in rare hard-cap races), there may be CLAIMED
        // tasks from a previous pass that couldn't be dispatched because no
        // semaphore permit was available. Try to dispatch them before claiming.
        let mut dispatched_buffered = 0u32;
        if claimed_count > 0 && self.semaphore.available_permits() > 0 {
            let buffered = self.broker.load_buffered_claimed(&self.worker_id).await?;
            for row in buffered {
                if self.semaphore.available_permits() == 0 {
                    break;
                }
                self.dispatch_task(row);
                dispatched_buffered += 1;
            }
        }

        if !can_claim_more {
            return Ok(dispatched_buffered > 0);
        }

        // Hard cap: no local permits means no new claims this pass.
        if !soft_cap_mode && self.semaphore.available_permits() == 0 {
            return Ok(dispatched_buffered > 0);
        }

        // ── Claim new tasks ──
        let claim_expires_at = self
            .app_config
            .claim_lease_ms
            .map(|ms| Utc::now() + chrono::Duration::milliseconds(ms as i64));

        // Pre-allocate with estimated capacity based on concurrency settings.
        let estimated_capacity = if soft_cap_mode {
            self.worker_config.concurrency + prefetch
        } else {
            self.semaphore.available_permits() as u32
        };
        let mut claimed_rows: Vec<crate::broker::ClaimedTaskRow> =
            Vec::with_capacity(estimated_capacity as usize);

        {
            // Serialize claim rounds with a global advisory transaction lock.
            let mut tx = self.broker.pool().begin().await?;
            let advisory_key = self.advisory_key_global();
            self.broker
                .advisory_xact_lock(&mut tx, advisory_key)
                .await?;

            let hard_cap_mode = !soft_cap_mode;
            let mut total_remaining = if hard_cap_mode {
                self.semaphore.available_permits() as u32
            } else {
                let running = self
                    .broker
                    .count_running_for_worker_tx(&mut tx, &self.worker_id)
                    .await? as u32;
                (self.worker_config.concurrency + prefetch).saturating_sub(running)
            };

            // Cluster-wide cap (hard cap mode only).
            if hard_cap_mode {
                if let Some(cap) = self.app_config.cluster_wide_cap {
                    let global_in_flight =
                        self.broker.count_global_in_flight_tx(&mut tx).await? as u32;
                    let global_remaining = cap.saturating_sub(global_in_flight);
                    total_remaining = total_remaining.min(global_remaining);
                }
            }

            if total_remaining == 0 {
                tx.commit().await?;
                return Ok(dispatched_buffered > 0);
            }

            for queue in self.ordered_queues() {
                if total_remaining == 0 {
                    break;
                }

                let mut per_queue_cap = if self.worker_config.queue_priorities.is_empty() {
                    // Round-robin fairness.
                    self.worker_config.max_claim_batch
                } else {
                    // Priority mode: fill greedily.
                    total_remaining
                };

                if !self.worker_config.queue_priorities.is_empty() {
                    if let Some(&max_q) = self.worker_config.queue_max_concurrency.get(&queue) {
                        let in_flight_q = if hard_cap_mode {
                            self.broker
                                .count_in_flight_for_queue_tx(&mut tx, &queue)
                                .await? as u32
                        } else {
                            self.broker
                                .count_running_for_queue_tx(&mut tx, &queue)
                                .await? as u32
                        };
                        let q_remaining = max_q.saturating_sub(in_flight_q);
                        per_queue_cap = per_queue_cap.min(q_remaining);
                    }
                }

                let to_claim = total_remaining.min(per_queue_cap);
                if to_claim == 0 {
                    continue;
                }

                let rows = self
                    .broker
                    .claim_in_tx(
                        &mut tx,
                        &queue,
                        to_claim as i32,
                        &self.worker_id,
                        claim_expires_at,
                    )
                    .await?;

                if !rows.is_empty() {
                    total_remaining = total_remaining.saturating_sub(rows.len() as u32);
                    claimed_rows.extend(rows);
                }
            }

            tx.commit().await?;
        }

        if claimed_rows.is_empty() {
            return Ok(dispatched_buffered > 0);
        }

        // Post-claim non-runnable workflow filter.
        // PAUSED: unclaim task → PENDING, reset workflow_task → READY
        // CANCELLED: cancel task → CANCELLED, skip workflow_task → SKIPPED
        if !claimed_rows.is_empty() {
            let all_ids: Vec<String> = claimed_rows.iter().map(|r| r.id.clone()).collect();
            let filtered_ids = self
                .broker
                .filter_non_runnable_workflow_tasks(&all_ids)
                .await?;
            if !filtered_ids.is_empty() {
                let filtered_set: std::collections::HashSet<&str> =
                    filtered_ids.iter().map(|s| s.as_str()).collect();
                claimed_rows.retain(|r| !filtered_set.contains(r.id.as_str()));
            }
        }

        let dispatched_claimed = claimed_rows.len() as u32;
        for row in claimed_rows {
            self.dispatch_task(row);
        }

        Ok(dispatched_claimed > 0 || dispatched_buffered > 0)
    }

    /// Order queues by priority (if configured) or return as-is.
    fn ordered_queues(&self) -> Vec<String> {
        if self.worker_config.queue_priorities.is_empty() {
            return self.worker_config.queues.clone();
        }

        let mut queues: Vec<String> = self
            .worker_config
            .queues
            .iter()
            .filter(|q| self.worker_config.queue_priorities.contains_key(*q))
            .cloned()
            .collect();
        queues.sort_by_key(|q| {
            self.worker_config
                .queue_priorities
                .get(q)
                .copied()
                .unwrap_or(i32::MAX)
        });
        queues
    }

    /// Compute a stable advisory lock key for claim serialization.
    fn advisory_key_global(&self) -> i64 {
        let basis = if self.app_config.broker.database_url.is_empty() {
            "horsies"
        } else {
            self.app_config.broker.database_url.as_str()
        };
        let mut hasher = Sha256::new();
        hasher.update(b"horsies-global:");
        hasher.update(basis.as_bytes());
        let digest = hasher.finalize();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&digest[..8]);
        i64::from_be_bytes(bytes)
    }

    /// Dispatch a single claimed task for execution.
    fn dispatch_task(&self, row: ClaimedTaskRow) {
        let Ok(permit) = Arc::clone(&self.semaphore).try_acquire_owned() else {
            if self.app_config.prefetch_buffer > 0 {
                tracing::debug!(
                    task_id = %row.id,
                    "no semaphore permit available, leaving task CLAIMED in prefetch buffer",
                );
            } else {
                tracing::warn!(
                    task_id = %row.id,
                    "no semaphore permit available, requeueing task",
                );
                let broker = Arc::clone(&self.broker);
                let task_id = row.id.clone();
                let worker_id = self.worker_id.clone();
                self.tracker.spawn(async move {
                    if let Err(e) = execution::unclaim_task_with_retry(
                        &broker,
                        &task_id,
                        &worker_id,
                        "no semaphore permit available",
                    )
                    .await
                    {
                        tracing::error!(
                            task_id = %task_id,
                            error = %e,
                            "failed to unclaim task after dispatch backpressure",
                        );
                    }
                });
            }
            return;
        };

        // Look up the registered task function.
        let task_fn = match self.registry.get(&row.task_name) {
            Ok(t) => t.clone(),
            Err(e) => {
                tracing::error!(
                    task_id = %row.id,
                    task_name = %row.task_name,
                    error = %e,
                    "task not registered",
                );
                // Fail the task immediately. Task is CLAIMED (not RUNNING),
                // so use a direct UPDATE that accepts CLAIMED status.
                let broker = Arc::clone(&self.broker);
                let workflow_registry = Arc::clone(&self.workflow_registry);
                let task_id = row.id.clone();
                let task_name = row.task_name.clone();
                let worker_id = self.worker_id.clone();
                let hostname = self.hostname.clone();
                self.tracker.spawn(async move {
                    let reason = format!("task '{}' not registered", task_name);
                    let task_error = TaskError::builtin(
                        OperationalErrorCode::WorkerResolutionError,
                        reason.clone(),
                    );
                    if let Some(work) = execution::finalize_pre_execution_failure(
                        Arc::clone(&broker),
                        row,
                        worker_id,
                        hostname,
                        task_error,
                    )
                    .await
                    {
                        execution::run_phase2(broker.pool(), &workflow_registry, work).await;
                    } else {
                        tracing::warn!(
                            task_id = %task_id,
                            reason,
                            "task resolution failure aborted before terminal state was persisted",
                        );
                    }
                    drop(permit);
                });
                return;
            }
        };

        let broker = Arc::clone(&self.broker);
        let workflow_registry = Arc::clone(&self.workflow_registry);
        let worker_id = self.worker_id.clone();
        let hostname = self.hostname.clone();
        let recovery = self.app_config.recovery.clone();

        self.tracker.spawn(async move {
            let phase2_work = execution::execute_and_finalize(
                Arc::clone(&broker),
                task_fn,
                row,
                worker_id,
                hostname,
                recovery,
            )
            .await;

            // Release permit immediately after Phase 1 completes,
            // before potentially slow Phase 2 workflow callbacks.
            drop(permit);

            if let Some(work) = phase2_work {
                execution::run_phase2(broker.pool(), &workflow_registry, work).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::execution::{
        execute_and_finalize, finalize_with_retry, notify_worker_capacity, persist_terminal_state,
        run_phase2, FinalizeStage, FINALIZE_MAX_RETRIES,
    };

    use crate::async_task_fn;
    use crate::broker::{ClaimedTaskRow, NotifyListener, PostgresBroker};
    use crate::core::config::recovery::RecoveryConfig;
    use crate::core::registry::WorkflowSpecRegistry;
    use crate::core::task::fn_trait::{AsyncTaskFn, RawTaskResult, RegisteredTask, TaskMeta};
    use crate::core::task::{OperationalErrorCode, TaskError, TaskErrorCode, TaskResult};
    use chrono::Utc;
    use futures::FutureExt;
    use serial_test::serial;
    use sqlx::PgPool;
    use std::future::Future;
    use std::panic::{resume_unwind, AssertUnwindSafe};
    use std::pin::Pin;
    use std::sync::Arc;
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
        let url = test_db_url();
        let pool = PgPool::connect(&url).await.expect("failed to connect");
        crate::broker::migrations::run_horsies_migrations(&pool)
            .await
            .expect("migrations failed");
        pool
    }

    async fn clean(pool: &PgPool) {
        sqlx::query(
            "TRUNCATE horsies_task_attempts, horsies_workflow_tasks, horsies_workflows, \
             horsies_tasks, horsies_heartbeats, horsies_worker_states, horsies_schedule_state CASCADE",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn test_broker() -> Arc<PostgresBroker> {
        let url = test_db_url();
        Arc::new(PostgresBroker::connect(&url).await.unwrap())
    }

    async fn insert_claimed_task(
        pool: &PgPool,
        task_id: &str,
        queue_name: &str,
        retry_count: i32,
        max_retries: i32,
        task_options: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, claimed_at, created_at, updated_at, claimed,
                claimed_by_worker_id, retry_count, max_retries, task_options,
                enqueue_sha
            ) VALUES (
                $1, 'finalize_test', $2, 100, '[]', '{}', 'CLAIMED',
                NOW(), NOW(), NOW(), NOW(), TRUE,
                'worker-1', $3, $4, $5,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
            )",
        )
        .bind(task_id)
        .bind(queue_name)
        .bind(retry_count)
        .bind(max_retries)
        .bind(task_options)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_running_task(
        pool: &PgPool,
        task_id: &str,
        queue_name: &str,
        retry_count: i32,
        max_retries: i32,
        task_options: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, started_at, created_at, updated_at, claimed,
                retry_count, max_retries, task_options, enqueue_sha
            ) VALUES (
                $1, 'finalize_test', $2, 100, '[]', '{}', 'RUNNING',
                NOW(), NOW(), NOW(), NOW(), FALSE,
                $3, $4, $5,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
            )",
        )
        .bind(task_id)
        .bind(queue_name)
        .bind(retry_count)
        .bind(max_retries)
        .bind(task_options)
        .execute(pool)
        .await
        .unwrap();
    }

    fn claimed_task_row(
        task_id: &str,
        queue_name: &str,
        retry_count: i32,
        max_retries: i32,
        task_options: Option<String>,
    ) -> ClaimedTaskRow {
        ClaimedTaskRow {
            id: task_id.to_owned(),
            task_name: "finalize_test".to_owned(),
            args: Some("[]".to_owned()),
            kwargs: Some("{}".to_owned()),
            retry_count,
            max_retries,
            task_options,
            queue_name: queue_name.to_owned(),
            good_until: None,
        }
    }

    async fn link_task_to_workflow(
        pool: &PgPool,
        task_id: &str,
        output_task_index: Option<i32>,
    ) -> String {
        let workflow_id = Uuid::new_v4().to_string();
        let workflow_task_id = Uuid::new_v4().to_string();

        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES (
                $1, 'finalize_wf', 'RUNNING', 'fail', $2,
                'test.finalize.v1', 0, $1,
                NOW(), NOW(), NOW(), NOW()
            )",
        )
        .bind(&workflow_id)
        .bind(output_task_index)
        .execute(pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args, task_kwargs,
                queue_name, priority, dependencies, allow_failed_deps, join_type,
                status, is_subworkflow, task_id, created_at
            ) VALUES (
                $1, $2, 0, 'node_0', 'finalize_test', '[]', '{}',
                'default', 100, '{}', FALSE, 'all',
                'ENQUEUED', FALSE, $3, NOW()
            )",
        )
        .bind(&workflow_task_id)
        .bind(&workflow_id)
        .bind(task_id)
        .execute(pool)
        .await
        .unwrap();

        workflow_id
    }

    async fn fetch_task_state(
        pool: &PgPool,
        task_id: &str,
    ) -> (String, Option<String>, Option<String>) {
        sqlx::query_as("SELECT status, result, error_code FROM horsies_tasks WHERE id = $1")
            .bind(task_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn fetch_attempt_state(pool: &PgPool, task_id: &str) -> (String, bool, Option<String>) {
        sqlx::query_as(
            "SELECT outcome, will_retry, error_code
             FROM horsies_task_attempts
             WHERE task_id = $1
             ORDER BY attempt
             LIMIT 1",
        )
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn fetch_workflow_state(pool: &PgPool, workflow_id: &str) -> (String, Option<String>) {
        sqlx::query_as("SELECT status, result FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn fetch_workflow_task_status(pool: &PgPool, workflow_id: &str, task_id: &str) -> String {
        sqlx::query_scalar(
            "SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_id = $2",
        )
        .bind(workflow_id)
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn fetch_only_workflow_task_state(
        pool: &PgPool,
        workflow_id: &str,
    ) -> (String, Option<String>) {
        sqlx::query_as(
            "SELECT status, task_id
             FROM horsies_workflow_tasks
             WHERE workflow_id = $1
             LIMIT 1",
        )
        .bind(workflow_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn fetch_attempt_count(pool: &PgPool, task_id: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn install_fail_workflow_task_running_trigger(pool: &PgPool) -> (String, String) {
        let suffix = Uuid::new_v4().simple().to_string();
        let function_name = format!("horsies_test_fail_wf_running_{}", suffix);
        let trigger_name = format!("horsies_test_fail_wf_running_trigger_{}", suffix);

        sqlx::query(&format!(
            "CREATE OR REPLACE FUNCTION {function_name}() RETURNS trigger AS $$
             BEGIN
                 IF NEW.status = 'RUNNING' THEN
                     RAISE EXCEPTION 'forced workflow_task RUNNING failure';
                 END IF;
                 RETURN NEW;
             END;
             $$ LANGUAGE plpgsql"
        ))
        .execute(pool)
        .await
        .unwrap();

        sqlx::query(&format!(
            "CREATE TRIGGER {trigger_name}
             BEFORE UPDATE ON horsies_workflow_tasks
             FOR EACH ROW
             WHEN (OLD.status IS DISTINCT FROM NEW.status)
             EXECUTE FUNCTION {function_name}()"
        ))
        .execute(pool)
        .await
        .unwrap();

        (function_name, trigger_name)
    }

    async fn remove_test_trigger(pool: &PgPool, function_name: &str, trigger_name: &str) {
        sqlx::query(&format!(
            "DROP TRIGGER IF EXISTS {trigger_name} ON horsies_workflow_tasks"
        ))
        .execute(pool)
        .await
        .unwrap();

        sqlx::query(&format!("DROP FUNCTION IF EXISTS {function_name}()"))
            .execute(pool)
            .await
            .unwrap();
    }

    #[test]
    fn worker_sync_api_compiles() {
        let _: fn(&Worker) = |w| w.request_stop();
        let _: fn(&Worker) -> CancellationToken = |w| w.cancel_token();
        let _: fn(&Worker) -> &str = |w| w.worker_id();
    }

    #[test]
    fn default_worker_config_is_valid() {
        let config = WorkerConfig::default();
        assert!(config.validate().is_ok());
        assert!(!config.queues.is_empty());
        assert!(config.concurrency >= 1);
        assert!(config.max_claim_batch >= 1);
    }

    #[test]
    fn worker_config_custom_queues() {
        let config = WorkerConfig {
            queues: vec!["high".to_owned(), "low".to_owned()],
            concurrency: 4,
            max_claim_batch: 5,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.queues.len(), 2);
        assert_eq!(config.concurrency, 4);
        assert_eq!(config.max_claim_batch, 5);
    }

    #[test]
    fn worker_config_with_priorities() {
        let mut priorities = std::collections::HashMap::new();
        priorities.insert("high".to_owned(), 1);
        priorities.insert("low".to_owned(), 10);

        let config = WorkerConfig {
            queues: vec!["high".to_owned(), "low".to_owned()],
            queue_priorities: priorities,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.queue_priorities.len(), 2);
        assert_eq!(config.queue_priorities["high"], 1);
    }

    #[test]
    fn worker_config_with_max_concurrency() {
        let mut max_conc = std::collections::HashMap::new();
        max_conc.insert("default".to_owned(), 2);

        let config = WorkerConfig {
            queue_max_concurrency: max_conc,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.queue_max_concurrency["default"], 2);
    }

    // -- finalize_with_retry unit tests --

    use std::sync::atomic::{AtomicU32, Ordering};

    static EXECUTION_COUNT: AtomicU32 = AtomicU32::new(0);

    fn transient_error() -> crate::broker::BrokerError {
        crate::broker::BrokerError::ConnectionFailed("simulated transient".to_owned())
    }

    fn non_retryable_error() -> crate::broker::BrokerError {
        crate::broker::BrokerError::InvalidStatus("simulated non-retryable".to_owned())
    }

    async fn succeed(_: ()) -> Result<String, TaskError> {
        Ok("done".to_owned())
    }

    async fn retryable_failure(_: ()) -> Result<String, TaskError> {
        Err(TaskError {
            error_code: Some(TaskErrorCode::User("RETRY_ME".to_owned())),
            message: Some("retry me".to_owned()),
            cause: None,
            data: None,
        })
    }

    async fn fatal_failure(_: ()) -> Result<String, TaskError> {
        Err(TaskError {
            error_code: Some(TaskErrorCode::User("FATAL".to_owned())),
            message: Some("boom".to_owned()),
            cause: None,
            data: None,
        })
    }

    async fn callback_success_task(_: ()) -> Result<String, TaskError> {
        Ok("workflow-done".to_owned())
    }

    async fn counted_success_task(_: ()) -> Result<String, TaskError> {
        EXECUTION_COUNT.fetch_add(1, Ordering::SeqCst);
        Ok("counted".to_owned())
    }

    async fn panic_task(_: ()) -> Result<String, TaskError> {
        panic!("task panicked before finalize");
    }

    struct InvalidJsonTask;

    impl AsyncTaskFn for InvalidJsonTask {
        fn execute(
            &self,
            _args: &[u8],
        ) -> Pin<Box<dyn Future<Output = RawTaskResult> + Send + '_>> {
            Box::pin(async { TaskResult::Ok(b"not-json".to_vec()) })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn finalize_retry_succeeds_on_first_attempt() {
        let result = finalize_with_retry("task-1", "test", || async { Ok::<i32, _>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test(start_paused = true)]
    async fn finalize_retry_recovers_after_transient_error() {
        let call_count = AtomicU32::new(0);
        let result = finalize_with_retry("task-2", "test", || {
            let n = call_count.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(transient_error())
                } else {
                    Ok::<i32, _>(99)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 99);
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            3,
            "should have tried 3 times"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn finalize_retry_exhausted_returns_error() {
        let call_count = AtomicU32::new(0);
        let result = finalize_with_retry("task-3", "test", || {
            call_count.fetch_add(1, Ordering::SeqCst);
            async { Err::<i32, _>(transient_error()) }
        })
        .await;
        assert!(
            result.is_err(),
            "should return error after exhausting retries"
        );
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            FINALIZE_MAX_RETRIES,
            "should have tried exactly FINALIZE_MAX_RETRIES times",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn finalize_retry_non_retryable_fails_immediately() {
        let call_count = AtomicU32::new(0);
        let result = finalize_with_retry("task-4", "test", || {
            call_count.fetch_add(1, Ordering::SeqCst);
            async { Err::<i32, _>(non_retryable_error()) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "non-retryable error should not trigger retry",
        );
    }

    /// Helper: run execute_and_finalize + Phase 2 with an empty workflow registry.
    async fn run_finalize(
        broker: &Arc<PostgresBroker>,
        task_fn: RegisteredTask,
        row: ClaimedTaskRow,
    ) {
        let phase2_work = execute_and_finalize(
            Arc::clone(broker),
            task_fn,
            row,
            "worker-1".to_owned(),
            "localhost".to_owned(),
            RecoveryConfig::default(),
        )
        .await;

        if let Some(work) = phase2_work {
            let registry = WorkflowSpecRegistry::new();
            run_phase2(broker.pool(), &registry, work).await;
        }
    }

    #[tokio::test]
    #[serial]
    async fn finalize_success_completes_task_and_records_attempt() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4().to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;

        run_finalize(
            &broker,
            async_task_fn!(succeed, ()),
            claimed_task_row(&task_id, "default", 0, 0, None),
        )
        .await;

        let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "COMPLETED");
        assert_eq!(error_code, None);
        let persisted: TaskResult<serde_json::Value> =
            serde_json::from_str(result.as_deref().unwrap()).unwrap();
        assert_eq!(
            persisted.unwrap(),
            serde_json::Value::String("done".to_owned())
        );

        let (outcome, will_retry, attempt_error_code) = fetch_attempt_state(&pool, &task_id).await;
        assert_eq!(outcome, "COMPLETED");
        assert!(!will_retry);
        assert_eq!(attempt_error_code, None);
    }

    #[tokio::test]
    #[serial]
    async fn finalize_success_advances_workflow_end_to_end() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4().to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        let workflow_id = link_task_to_workflow(&pool, &task_id, Some(0)).await;

        run_finalize(
            &broker,
            async_task_fn!(callback_success_task, ()),
            claimed_task_row(&task_id, "default", 0, 0, None),
        )
        .await;

        let (task_status, _, _) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(task_status, "COMPLETED");

        // Phase 2 runs internally — workflow should be advanced.
        let workflow_task_status = fetch_workflow_task_status(&pool, &workflow_id, &task_id).await;
        assert_eq!(workflow_task_status, "COMPLETED");

        let (workflow_status, workflow_result) = fetch_workflow_state(&pool, &workflow_id).await;
        assert_eq!(workflow_status, "COMPLETED");
        let workflow_result: TaskResult<serde_json::Value> =
            serde_json::from_str(workflow_result.as_deref().unwrap()).unwrap();
        assert_eq!(
            workflow_result.unwrap(),
            serde_json::Value::String("workflow-done".to_owned())
        );
    }

    #[tokio::test]
    #[serial]
    async fn finalize_phase1_commit_is_durable_before_phase2_runs() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4().to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        let workflow_id = link_task_to_workflow(&pool, &task_id, Some(0)).await;

        let phase2_work = execute_and_finalize(
            Arc::clone(&broker),
            async_task_fn!(callback_success_task, ()),
            claimed_task_row(&task_id, "default", 0, 0, None),
            "worker-1".to_owned(),
            "localhost".to_owned(),
            RecoveryConfig::default(),
        )
        .await
        .expect("terminal success should produce phase 2 work");

        let (task_status, _, _) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(task_status, "COMPLETED");

        let workflow_task_status = fetch_workflow_task_status(&pool, &workflow_id, &task_id).await;
        assert_eq!(
            workflow_task_status, "RUNNING",
            "phase 1 should commit task durability before phase 2 advances workflow state",
        );

        let (workflow_status, workflow_result) = fetch_workflow_state(&pool, &workflow_id).await;
        assert_eq!(workflow_status, "RUNNING");
        assert_eq!(workflow_result, None);

        let registry = WorkflowSpecRegistry::new();
        run_phase2(broker.pool(), &registry, phase2_work).await;

        let workflow_task_status = fetch_workflow_task_status(&pool, &workflow_id, &task_id).await;
        assert_eq!(workflow_task_status, "COMPLETED");

        let (workflow_status, workflow_result) = fetch_workflow_state(&pool, &workflow_id).await;
        assert_eq!(workflow_status, "COMPLETED");
        let workflow_result: TaskResult<serde_json::Value> =
            serde_json::from_str(workflow_result.as_deref().unwrap()).unwrap();
        assert_eq!(
            workflow_result.unwrap(),
            serde_json::Value::String("workflow-done".to_owned())
        );
    }

    #[tokio::test]
    #[serial]
    async fn finalize_phase2_failure_keeps_terminal_task_durable() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4().to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        let workflow_id = link_task_to_workflow(&pool, &task_id, Some(0)).await;

        let phase2_work = execute_and_finalize(
            Arc::clone(&broker),
            async_task_fn!(callback_success_task, ()),
            claimed_task_row(&task_id, "default", 0, 0, None),
            "worker-1".to_owned(),
            "localhost".to_owned(),
            RecoveryConfig::default(),
        )
        .await
        .expect("terminal success should produce phase 2 work");

        let failed_phase2_pool = test_pool().await;
        failed_phase2_pool.close().await;

        let registry = WorkflowSpecRegistry::new();
        run_phase2(&failed_phase2_pool, &registry, phase2_work).await;

        let (task_status, result_json, _) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(task_status, "COMPLETED");
        let persisted: TaskResult<serde_json::Value> =
            serde_json::from_str(result_json.as_deref().unwrap()).unwrap();
        assert_eq!(
            persisted.unwrap(),
            serde_json::Value::String("workflow-done".to_owned())
        );

        let workflow_task_status = fetch_workflow_task_status(&pool, &workflow_id, &task_id).await;
        assert_eq!(workflow_task_status, "RUNNING");

        let (workflow_status, workflow_result) = fetch_workflow_state(&pool, &workflow_id).await;
        assert_eq!(workflow_status, "RUNNING");
        assert_eq!(workflow_result, None);
    }

    #[tokio::test]
    #[serial]
    async fn finalize_prestart_workflow_update_failure_aborts_before_user_code() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4().to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        link_task_to_workflow(&pool, &task_id, Some(0)).await;
        EXECUTION_COUNT.store(0, Ordering::SeqCst);

        let (function_name, trigger_name) = install_fail_workflow_task_running_trigger(&pool).await;
        let result = AssertUnwindSafe(async {
            run_finalize(
                &broker,
                async_task_fn!(counted_success_task, ()),
                claimed_task_row(&task_id, "default", 0, 0, None),
            )
            .await;

            assert_eq!(
                EXECUTION_COUNT.load(Ordering::SeqCst),
                0,
                "user code must not run when workflow_task RUNNING sync fails",
            );

            let broker_error_code = OperationalErrorCode::BrokerError.to_string();
            let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
            assert_eq!(status, "FAILED");
            assert_eq!(error_code.as_deref(), Some(broker_error_code.as_str()));

            let persisted: TaskResult<serde_json::Value> =
                serde_json::from_str(result.as_deref().unwrap()).unwrap();
            let persisted_error = persisted.unwrap_err();
            assert_eq!(
                persisted_error.error_code,
                Some(OperationalErrorCode::BrokerError.into())
            );
            assert!(persisted_error.message.as_deref().is_some_and(|message| {
                message.contains("failed to update workflow task to RUNNING")
            }));

            let attempt_count = fetch_attempt_count(&pool, &task_id).await;
            assert_eq!(attempt_count, 1);
        })
        .catch_unwind()
        .await;

        remove_test_trigger(&pool, &function_name, &trigger_name).await;

        if let Err(panic) = result {
            resume_unwind(panic);
        }
    }

    #[tokio::test]
    #[serial]
    async fn finalize_paused_workflow_requeues_without_running_user_code() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4().to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        let workflow_id = link_task_to_workflow(&pool, &task_id, Some(0)).await;
        sqlx::query("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = $1")
            .bind(&workflow_id)
            .execute(&pool)
            .await
            .unwrap();
        EXECUTION_COUNT.store(0, Ordering::SeqCst);

        run_finalize(
            &broker,
            async_task_fn!(counted_success_task, ()),
            claimed_task_row(&task_id, "default", 0, 0, None),
        )
        .await;

        assert_eq!(EXECUTION_COUNT.load(Ordering::SeqCst), 0);

        let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "PENDING");
        assert_eq!(result, None);
        assert_eq!(error_code, None);
        let (workflow_task_status, workflow_task_id) =
            fetch_only_workflow_task_state(&pool, &workflow_id).await;
        assert_eq!(workflow_task_status, "READY");
        assert_eq!(workflow_task_id, None);
        assert_eq!(fetch_attempt_count(&pool, &task_id).await, 0);
    }

    #[tokio::test]
    #[serial]
    async fn finalize_cancelled_workflow_skips_without_running_user_code() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4().to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        let workflow_id = link_task_to_workflow(&pool, &task_id, Some(0)).await;
        sqlx::query("UPDATE horsies_workflows SET status = 'CANCELLED' WHERE id = $1")
            .bind(&workflow_id)
            .execute(&pool)
            .await
            .unwrap();
        EXECUTION_COUNT.store(0, Ordering::SeqCst);

        run_finalize(
            &broker,
            async_task_fn!(counted_success_task, ()),
            claimed_task_row(&task_id, "default", 0, 0, None),
        )
        .await;

        assert_eq!(EXECUTION_COUNT.load(Ordering::SeqCst), 0);

        let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "CANCELLED");
        assert_eq!(result, None);
        assert_eq!(error_code, None);
        assert_eq!(
            fetch_workflow_task_status(&pool, &workflow_id, &task_id).await,
            "SKIPPED"
        );
        assert_eq!(fetch_attempt_count(&pool, &task_id).await, 0);
    }

    #[tokio::test]
    #[serial]
    async fn finalize_retryable_failure_requeues_and_skips_callback() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4().to_string();
        let task_options = serde_json::json!({
            "auto_retry_for": ["RETRY_ME"],
            "retry_policy": {
                "intervals": [1, 1, 1],
                "backoff_strategy": "fixed",
                "jitter": false
            }
        })
        .to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 3, Some(&task_options)).await;

        run_finalize(
            &broker,
            async_task_fn!(retryable_failure, ()),
            claimed_task_row(&task_id, "default", 0, 3, Some(task_options)),
        )
        .await;

        let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "PENDING");
        assert_eq!(result, None);
        assert_eq!(error_code, None);

        let retry_count: i32 =
            sqlx::query_scalar("SELECT retry_count FROM horsies_tasks WHERE id = $1")
                .bind(&task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(retry_count, 1);

        let next_retry_at: Option<chrono::DateTime<Utc>> =
            sqlx::query_scalar("SELECT next_retry_at FROM horsies_tasks WHERE id = $1")
                .bind(&task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            next_retry_at.is_some_and(|ts| ts > Utc::now()),
            "retry scheduling should persist a future next_retry_at"
        );

        let (outcome, will_retry, attempt_error_code) = fetch_attempt_state(&pool, &task_id).await;
        assert_eq!(outcome, "FAILED");
        assert!(will_retry);
        assert_eq!(attempt_error_code.as_deref(), Some("RETRY_ME"));
    }

    #[tokio::test]
    #[serial]
    async fn finalize_non_retryable_failure_records_terminal_state() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4().to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;

        run_finalize(
            &broker,
            async_task_fn!(fatal_failure, ()),
            claimed_task_row(&task_id, "default", 0, 0, None),
        )
        .await;

        let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "FAILED");
        assert_eq!(error_code.as_deref(), Some("FATAL"));
        let persisted: TaskResult<serde_json::Value> =
            serde_json::from_str(result.as_deref().unwrap()).unwrap();
        let persisted_error = persisted.unwrap_err();
        assert_eq!(
            persisted_error.error_code,
            Some(TaskErrorCode::User("FATAL".to_owned()))
        );

        let (outcome, will_retry, attempt_error_code) = fetch_attempt_state(&pool, &task_id).await;
        assert_eq!(outcome, "FAILED");
        assert!(!will_retry);
        assert_eq!(attempt_error_code.as_deref(), Some("FATAL"));
    }

    #[tokio::test]
    #[serial]
    async fn finalize_async_panic_records_terminal_failure() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;
        let task_error_code = OperationalErrorCode::TaskError.to_string();

        let task_id = Uuid::new_v4().to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;

        run_finalize(
            &broker,
            async_task_fn!(panic_task, ()),
            claimed_task_row(&task_id, "default", 0, 0, None),
        )
        .await;

        let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "FAILED");
        assert_eq!(error_code.as_deref(), Some(task_error_code.as_str()));

        let persisted: TaskResult<serde_json::Value> =
            serde_json::from_str(result.as_deref().unwrap()).unwrap();
        let persisted_error = persisted.unwrap_err();
        assert!(persisted_error
            .message
            .as_deref()
            .is_some_and(|msg| msg.contains("async task panicked")),);
    }

    #[tokio::test]
    #[serial]
    async fn finalize_phase1_db_failure_leaves_task_running_for_reaper() {
        let pool = test_pool().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4().to_string();
        insert_running_task(&pool, &task_id, "default", 0, 0, None).await;

        let closed_broker = test_broker().await;
        closed_broker.pool().close().await;

        let Err(err) = persist_terminal_state(
            &closed_broker,
            &task_id,
            TaskResult::Ok(br#""phase1-durable""#.to_vec()),
            &claimed_task_row(&task_id, "default", 0, 0, None),
            Utc::now(),
            "worker-1",
            "localhost",
        )
        .await
        else {
            panic!("closed broker should exhaust phase 1 retries");
        };

        assert_eq!(err.stage, FinalizeStage::Phase1Persist);
        assert!(err.retryable);

        let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "RUNNING");
        assert_eq!(result, None);
        assert_eq!(error_code, None);
    }

    #[tokio::test]
    #[serial]
    async fn finalize_invalid_ok_payload_falls_back_to_serialization_failure() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;
        let ser_error_code = OperationalErrorCode::WorkerSerializationError.to_string();

        let task_id = Uuid::new_v4().to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;

        run_finalize(
            &broker,
            RegisteredTask::Async {
                task: Arc::new(InvalidJsonTask),
                meta: TaskMeta::default(),
            },
            claimed_task_row(&task_id, "default", 0, 0, None),
        )
        .await;

        let (status, _, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "FAILED");
        assert_eq!(error_code.as_deref(), Some(ser_error_code.as_str()));

        let (outcome, will_retry, attempt_error_code) = fetch_attempt_state(&pool, &task_id).await;
        assert_eq!(outcome, "FAILED");
        assert!(!will_retry);
        assert_eq!(attempt_error_code.as_deref(), Some(ser_error_code.as_str()));
    }

    #[tokio::test]
    #[serial]
    async fn notify_worker_capacity_emits_global_and_queue_specific_signals() {
        let pool = test_pool().await;
        clean(&pool).await;

        let mut listener = NotifyListener::connect(&pool).await.unwrap();
        listener.listen("task_new").await.unwrap();
        listener.listen("task_queue_high-priority").await.unwrap();

        notify_worker_capacity(&pool, "high-priority", "task-123").await;

        let first = tokio::time::timeout(Duration::from_secs(2), listener.recv())
            .await
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), listener.recv())
            .await
            .unwrap()
            .unwrap();

        let mut seen = vec![
            (first.channel().to_owned(), first.payload().to_owned()),
            (second.channel().to_owned(), second.payload().to_owned()),
        ];
        seen.sort();

        assert_eq!(
            seen,
            vec![
                ("task_new".to_owned(), "capacity:task-123".to_owned(),),
                (
                    "task_queue_high-priority".to_owned(),
                    "capacity:task-123".to_owned(),
                ),
            ]
        );
    }

    #[tokio::test]
    #[serial]
    async fn notify_worker_capacity_swallow_pool_errors() {
        let pool = test_pool().await;
        pool.close().await;
        notify_worker_capacity(&pool, "default", "task-closed").await;
    }
}

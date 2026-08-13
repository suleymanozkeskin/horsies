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

/// Decode a liveness ping and reply when it is a broadcast or addressed to
/// this worker.
///
/// A reply proves this worker's event loop is responsive and that it can reach
/// Postgres (the pong travels through `pool`). Malformed pings and pings
/// addressed to another worker are dropped silently. Mirrors Python's
/// `_handle_ping`.
/// Derive a stable 64-bit advisory key from a label (first 8 bytes of SHA-256).
fn advisory_key_from(label: &str) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(label.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(bytes)
}

/// The global claim-serialization key.
///
/// A constant key (not derived from the DSN) so every worker on the same database
/// contends on the same lock regardless of DSN spelling (host vs IP, pooler
/// frontend vs direct). Parity with horsies PR #101 c1c9e2d8.
fn advisory_key_global() -> i64 {
    advisory_key_from("horsies:claim:v1")
}

/// A per-queue claim-serialization key (separate hash namespace from the global
/// key so the two families never collide). Parity with horsies PR #125.
fn advisory_key_queue(queue: &str) -> i64 {
    advisory_key_from(&format!("horsies:claim:queue:v1:{}", queue))
}

/// Advisory lock keys a claim pass must hold for consistent cap accounting.
///
/// The lock exists only to make cap accounting consistent across workers; when no
/// cap applies, `FOR UPDATE SKIP LOCKED` alone prevents double-claims (empty result).
///
/// - `cluster_wide_cap` set → the single global key (the global in-flight count is
///   a cluster invariant that subsumes per-queue caps).
/// - else → one key per **capped queue in the actual claim list** (a queue present
///   in `queue_max_concurrency`), sorted ascending so multi-key acquisition is
///   deadlock-free, and deduped. Workers claiming disjoint capped queues do not
///   serialize against each other.
/// - else (no caps configured) → no keys.
///
/// Per-queue caps are keyed on `queue_max_concurrency` membership, independent of
/// `queue_priorities`, so a cap is enforced in both round-robin and priority modes.
/// Rolling-deploy skew: old workers (global key) and new workers (per-queue keys)
/// don't contend, so a per-queue cap can briefly overshoot by up to one pass's
/// batch during a rollout.
/// Queues a claim pass services, in claim order.
///
/// Round-robin mode (no `queue_priorities`) keeps the configured order and
/// services every listed queue; per-queue caps are enforced downstream via
/// `queue_max_concurrency` membership, so no filtering is needed. Priority
/// mode keeps queues carrying a priority OR a max_concurrency cap and sorts
/// by priority — a cap-only queue sorts into the default priority bucket
/// (100) so its cap is enforced rather than the queue being silently starved
/// (parity with horsies PR #166; sort is stable, ties keep configured order).
///
/// Divergence from Python #166: a cap-only config (caps set, no priorities)
/// stays in round-robin mode here and still services uncapped queues; Python
/// switches to custom mode and drops them. Rust's cap accounting is keyed on
/// `queue_max_concurrency` alone, so caps are enforced in both modes.
fn compute_ordered_queues(
    queues: &[String],
    queue_priorities: &std::collections::HashMap<String, i32>,
    queue_max_concurrency: &std::collections::HashMap<String, u32>,
) -> Vec<String> {
    if queue_priorities.is_empty() {
        return queues.to_vec();
    }
    let mut kept: Vec<String> = queues
        .iter()
        .filter(|q| queue_priorities.contains_key(*q) || queue_max_concurrency.contains_key(*q))
        .cloned()
        .collect();
    kept.sort_by_key(|q| queue_priorities.get(q).copied().unwrap_or(100));
    kept
}

fn compute_claim_lock_keys(
    cluster_wide_cap: Option<u32>,
    ordered_queues: &[String],
    queue_max_concurrency: &std::collections::HashMap<String, u32>,
) -> Vec<i64> {
    if cluster_wide_cap.is_some() {
        return vec![advisory_key_global()];
    }
    let mut keys: Vec<i64> = ordered_queues
        .iter()
        .filter(|q| queue_max_concurrency.contains_key(*q))
        .map(|q| advisory_key_queue(q))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

async fn handle_worker_ping(
    pool: &sqlx::PgPool,
    worker_id: &str,
    hostname: &str,
    pid: i32,
    raw_payload: &str,
) -> Result<(), crate::broker::BrokerError> {
    let request: crate::broker::health::WorkerPingRequest = match serde_json::from_str(raw_payload)
    {
        Ok(request) => request,
        Err(e) => {
            tracing::warn!(error = %e, "discarding invalid ping payload");
            return Ok(());
        }
    };

    if let Some(target) = &request.target_worker_id {
        if target != worker_id {
            return Ok(()); // addressed to a different worker
        }
    }

    let pong = crate::broker::health::WorkerPongPayload {
        correlation_id: request.correlation_id,
        worker_id: worker_id.to_owned(),
        hostname: hostname.to_owned(),
        pid,
    };
    let payload =
        serde_json::to_string(&pong).map_err(crate::broker::BrokerError::Serialization)?;

    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(&request.reply_channel)
        .bind(&payload)
        .execute(pool)
        .await
        .map_err(crate::broker::BrokerError::Database)?;
    Ok(())
}

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
    /// Task ids currently handed to the tracker (dispatched, not yet finished).
    /// The buffered-dispatch probe skips these so a task still in its
    /// CLAIMED→RUNNING window is not re-fetched and re-dispatched next pass (P7).
    in_dispatch: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
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
            in_dispatch: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
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
        let ping_pool = self.broker.pool().clone();
        let ping_worker_id = self.worker_id.clone();
        let ping_hostname = self.hostname.clone();
        let ping_pid = std::process::id() as i32;
        let _listener_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = listener_cancel.cancelled() => break,
                    notification = listener.recv() => {
                        match notification {
                            Ok(notif) => {
                                if notif.channel() == crate::broker::health::WORKER_PING_CHANNEL {
                                    // Serve liveness pings off the lossy wake-up
                                    // path; spawn so recv() keeps draining.
                                    let pool = ping_pool.clone();
                                    let worker_id = ping_worker_id.clone();
                                    let hostname = ping_hostname.clone();
                                    let payload = notif.payload().to_owned();
                                    tokio::spawn(async move {
                                        if let Err(e) = handle_worker_ping(
                                            &pool, &worker_id, &hostname, ping_pid, &payload,
                                        )
                                        .await
                                        {
                                            tracing::error!(error = %e, "ping responder error");
                                        }
                                    });
                                } else if notify_tx.try_send(notif).is_err() {
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
        let claimer_hb = spawn_claimer_heartbeat(
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
        let reaper = spawn_reaper(
            self.broker.pool().clone(),
            self.app_config.recovery.clone(),
            self.app_config.retention.clone(),
            self.cancel.clone(),
        );

        // Start workflow recovery loop.
        let wf_recovery = spawn_workflow_recovery(
            self.broker.pool().clone(),
            Arc::clone(&self.workflow_registry),
            self.app_config.recovery.clone(),
            self.app_config.payload.clone(),
            self.app_config.retention.clone(),
            self.cancel.clone(),
        );

        // Start worker state snapshot loop.
        let worker_started_at = Utc::now();
        let state_loop = crate::worker::worker_state::spawn_worker_state_loop(
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

        // Supervise the service loops (C11-inverse): a loop that dies without
        // a shutdown request — panic or unexpected return; DB errors are
        // handled inside each loop — must not leave the worker claiming with
        // lease renewal, stale-task recovery, or liveness advertising dead.
        // The watchdog fires the cancellation token (claiming stops at once)
        // and the death surfaces below as `WorkerError::ServiceLoopDied`, so
        // the process exits non-zero. The listener task needs no watchdog:
        // its death drops `notify_tx` and the main loop already shuts down on
        // the closed channel.
        let (fatal_tx, mut fatal_rx) =
            mpsc::unbounded_channel::<crate::worker::supervision::ServiceExit>();
        let services: [(&'static str, tokio::task::JoinHandle<()>); 4] = [
            ("claimer-heartbeat", claimer_hb),
            ("reaper", reaper),
            ("workflow-recovery", wf_recovery),
            ("worker-state", state_loop),
        ];
        for (service, handle) in services {
            crate::worker::supervision::spawn_service_watchdog(
                service,
                handle,
                self.cancel.clone(),
                fatal_tx.clone(),
            );
        }
        drop(fatal_tx);

        tracing::info!(worker_id = %self.worker_id, "worker ready");

        // Main-loop claim error backoff.
        let mut claim_backoff = RetryBackoff::from_config(&self.app_config.resilience);

        // A fatal claim error (non-retryable, or retryable with the backoff
        // exhausted) breaks the loop and is returned after graceful shutdown so
        // the process exits non-zero for a supervisor restart — the same
        // discipline as Python's run_forever re-raise and the ServiceLoopDied
        // path below.
        let mut fatal_claim_error: Option<WorkerError> = None;

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
                        fatal_claim_error = Some(e);
                        break;
                    } else {
                        tracing::error!(
                            error = %e,
                            "non-retryable claim error — shutting down",
                        );
                        fatal_claim_error = Some(e);
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

        // Stop all background loops (listener, claimer heartbeat, reaper,
        // workflow recovery, worker-state) on every exit path — including a fatal
        // claim error that `break`s without a prior cancel. Otherwise those loops
        // keep running on the host runtime: the worker-state loop advertises the
        // dead worker as alive and the claimer heartbeat keeps renewing leases,
        // delaying reclamation of its CLAIMED tasks. Idempotent when a signal or
        // `request_stop` already fired the token (C11).
        self.cancel.cancel();

        // Graceful shutdown: wait for in-flight tasks.
        self.shutdown().await;

        // A fatal claim error broke the loop; surface it so the process exits
        // non-zero for a supervisor restart. In-flight tasks were still
        // drained above.
        if let Some(e) = fatal_claim_error {
            return Err(e);
        }

        // A service-loop death fired the token to get here; surface it so the
        // process exits non-zero instead of reporting a clean shutdown
        // (C11-inverse). In-flight tasks were still drained above.
        if let Ok(exit) = fatal_rx.try_recv() {
            return Err(WorkerError::ServiceLoopDied {
                service: exit.service,
                reason: exit.reason,
            });
        }
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
        self.broker.ensure_listener_delivery_checked().await?;
        let mut listener = NotifyListener::connect(self.broker.session_pool()).await?;
        for queue in &self.worker_config.queues {
            listener.listen(&format!("task_queue_{}", queue)).await?;
        }
        // The global task_new channel is deliberately NOT subscribed: the
        // per-queue channels cover every insert and capacity signal this worker
        // can act on, while task_new wakes every worker for every insert
        // cluster-wide (thundering herd). The trigger still emits task_new for
        // external observers. Parity with horsies PR #101 cafc9200.
        // Subscribe to the shared liveness-ping channel BEFORE any background
        // loop is spawned, so a subscription failure cannot orphan running loops.
        listener
            .listen(crate::broker::health::WORKER_PING_CHANNEL)
            .await?;
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
    /// Budget semantics, computed server-side by `horsies_claim` under the
    /// pass's advisory locks (parity with horsies PR #160):
    /// - **Hard cap** (`prefetch_buffer == 0`): budget =
    ///   `concurrency - (RUNNING + active CLAIMED)` for this worker.
    /// - **Soft cap / prefetch** (`prefetch_buffer > 0`): budget =
    ///   `concurrency + prefetch_buffer - RUNNING - CLAIMED`. Claims extra
    ///   tasks with a lease; they sit CLAIMED until a permit opens up.
    ///
    /// When `cluster_wide_cap` is set (hard cap mode only), the budget is
    /// further reduced so the total RUNNING + CLAIMED tasks across ALL
    /// workers does not exceed the cap.
    async fn claim_and_dispatch_all(&self) -> Result<bool, WorkerError> {
        let prefetch = self.app_config.prefetch_buffer;
        let soft_cap_mode = prefetch > 0;

        // ── Dispatch buffered CLAIMED tasks first ──
        // Only prefetch/soft-cap mode buffers CLAIMED tasks across passes (hard
        // cap requeues a task on a missing permit), so the buffered-dispatch
        // probe runs only there. The old per-pass `count_claimed_for_worker`
        // round trip gated this and the per-worker budget, but `horsies_claim`
        // recomputes that budget authoritatively under its advisory locks
        // (including `max_claim_per_worker`) and returns empty when exhausted, so
        // the client-side count was redundant (P5). `load_buffered_claimed`
        // itself returns empty when there is nothing buffered.
        let mut dispatched_buffered = 0u32;
        if soft_cap_mode && self.semaphore.available_permits() > 0 {
            // At most `available_permits` rows can be dispatched this pass, so
            // bound the fetch (which pulls full args/kwargs payloads) to that (P7).
            let permit_limit = self.semaphore.available_permits() as i64;
            let buffered = self
                .broker
                .load_buffered_claimed(&self.worker_id, permit_limit)
                .await?;

            // Soft-cap re-check: the claim function counts only RUNNING against a
            // queue's `max_concurrency`, so successive passes can buffer more
            // CLAIMED tasks of a capped queue than its cap while none are running.
            // Dispatching all of them would push queue-level RUNNING past the cap
            // by up to `prefetch_buffer` (C17). Gate capped-queue dispatch on the
            // cluster-wide RUNNING count so a buffered task never starts beyond
            // the cap; the excess stays CLAIMED (leased) until running frees up.
            let mut running_by_queue = std::collections::HashMap::new();
            if soft_cap_mode && !self.worker_config.queue_max_concurrency.is_empty() {
                let capped: Vec<String> = buffered
                    .iter()
                    .map(|r| r.queue_name.clone())
                    .filter(|q| self.worker_config.queue_max_concurrency.contains_key(q))
                    .collect();
                running_by_queue = self.broker.count_running_by_queue(&capped).await?;
            }
            let mut dispatched_by_queue: std::collections::HashMap<String, u32> =
                std::collections::HashMap::new();

            for row in buffered {
                if self.semaphore.available_permits() == 0 {
                    break;
                }
                // Skip a task already handed to the tracker: it is still CLAIMED
                // in its set_running window and would be re-dispatched here (P7).
                if self
                    .in_dispatch
                    .lock()
                    .expect("in_dispatch poisoned")
                    .contains(&row.id.to_string())
                {
                    continue;
                }
                // In soft-cap mode, do not start a capped queue's buffered task
                // past its cluster-wide RUNNING cap.
                if soft_cap_mode {
                    if let Some(&cap) = self
                        .worker_config
                        .queue_max_concurrency
                        .get(&row.queue_name)
                    {
                        let running = running_by_queue.get(&row.queue_name).copied().unwrap_or(0);
                        let pending = *dispatched_by_queue.get(&row.queue_name).unwrap_or(&0);
                        if running + i64::from(pending) >= i64::from(cap) {
                            continue; // cap reached; leave this task CLAIMED
                        }
                        *dispatched_by_queue
                            .entry(row.queue_name.clone())
                            .or_insert(0) += 1;
                    }
                }
                self.dispatch_task(row);
                dispatched_buffered += 1;
            }
        }

        // Hard cap: no local permits means no new claims this pass.
        if !soft_cap_mode && self.semaphore.available_permits() == 0 {
            return Ok(dispatched_buffered > 0);
        }

        // ── Claim new tasks ──
        // The critical section (advisory-lock acquisition, cap accounting,
        // budget math, windowed claim) is one server-side statement —
        // `horsies_claim`, parity with horsies PR #160 — so the xact-scoped
        // locks are never held across a client round trip and a stalled
        // worker cannot freeze other claimers mid-pass. The checks above are
        // cheap local early-exits; the function recomputes the budget
        // authoritatively under the locks. The queue list drives the lock
        // keys, so the locks held and the queues claimed are provably the
        // same set.
        let ordered = self.ordered_queues();
        let queue_priority: std::collections::HashMap<String, i32> = ordered
            .iter()
            .map(|q| {
                let priority = self
                    .worker_config
                    .queue_priorities
                    .get(q)
                    .copied()
                    .unwrap_or(100);
                (q.clone(), priority)
            })
            .collect();
        let queue_max_concurrency: std::collections::HashMap<String, u32> = ordered
            .iter()
            .filter_map(|q| {
                self.worker_config
                    .queue_max_concurrency
                    .get(q)
                    .map(|&cap| (q.clone(), cap))
            })
            .collect();
        let params = crate::broker::ClaimPassParams {
            worker_id: self.worker_id.clone(),
            lock_keys: self.claim_lock_keys(&ordered),
            queues: ordered,
            queue_priority,
            queue_max_concurrency,
            hard_cap_mode: !soft_cap_mode,
            processes: self.worker_config.concurrency,
            prefetch_buffer: prefetch,
            max_claim_per_worker: self.worker_config.max_claim_per_worker,
            max_claim_batch: self.worker_config.max_claim_batch,
            cluster_wide_cap: self.app_config.cluster_wide_cap,
            claim_lease_ms: self.app_config.claim_lease_ms,
        };
        let mut claimed_rows = self.broker.claim_batch(&params).await?;

        if claimed_rows.is_empty() {
            return Ok(dispatched_buffered > 0);
        }

        // Post-claim non-runnable workflow filter.
        // PAUSED: cancel task → CANCELLED (terminal), reset workflow_task → READY
        // CANCELLED: cancel task → CANCELLED, skip workflow_task → SKIPPED
        //
        // Only workflow tasks can belong to a PAUSED/CANCELLED workflow, so a
        // plain-task-only batch cannot be filtered — skip the guaranteed-empty
        // JOIN query in that case (P5). `is_workflow_task` is carried on each
        // claimed row.
        if claimed_rows.iter().any(|r| r.is_workflow_task) {
            // The claim generations travel with their ids: one batch can span
            // claim transactions, so each task fences on its own claimed_at.
            let claims: Vec<(String, Option<chrono::DateTime<chrono::Utc>>)> = claimed_rows
                .iter()
                .map(|r| (r.id.to_string(), r.claimed_at))
                .collect();
            let filtered_ids = self
                .broker
                .filter_non_runnable_workflow_tasks(&claims, &self.worker_id)
                .await?;
            if !filtered_ids.is_empty() {
                let filtered_set: std::collections::HashSet<&str> =
                    filtered_ids.iter().map(|s| s.as_str()).collect();
                claimed_rows.retain(|r| !filtered_set.contains(r.id.to_string().as_str()));
            }
        }

        let dispatched_claimed = claimed_rows.len() as u32;
        for row in claimed_rows {
            self.dispatch_task(row);
        }

        Ok(dispatched_claimed > 0 || dispatched_buffered > 0)
    }

    /// Order queues by priority (if configured) or return as-is.
    ///
    /// See [`compute_ordered_queues`] for the policy.
    fn ordered_queues(&self) -> Vec<String> {
        compute_ordered_queues(
            &self.worker_config.queues,
            &self.worker_config.queue_priorities,
            &self.worker_config.queue_max_concurrency,
        )
    }

    /// Advisory lock keys this claim pass must hold for consistent cap accounting.
    ///
    /// See [`compute_claim_lock_keys`] for the policy. An empty result means
    /// `FOR UPDATE SKIP LOCKED` alone is sufficient and no lock is taken.
    fn claim_lock_keys(&self, ordered_queues: &[String]) -> Vec<i64> {
        compute_claim_lock_keys(
            self.app_config.cluster_wide_cap,
            ordered_queues,
            &self.worker_config.queue_max_concurrency,
        )
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
                let task_id = row.id.to_string();
                let worker_id = self.worker_id.clone();
                let claimed_at = row.claimed_at;
                self.tracker.spawn(async move {
                    if let Err(e) = execution::unclaim_task_with_retry(
                        &broker,
                        &task_id,
                        &worker_id,
                        claimed_at,
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
                let orphan_self_heal = self
                    .app_config
                    .recovery
                    .auto_terminate_orphaned_workflow_tasks;
                let hostname = self.hostname.clone();
                let payload_policy = self.app_config.payload.clone();
                let phase2_policy = self.app_config.payload.clone();
                let phase2_retention = self.app_config.retention.clone();
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
                        payload_policy,
                        orphan_self_heal,
                    )
                    .await
                    {
                        execution::run_phase2(
                            broker.pool(),
                            &workflow_registry,
                            work,
                            &phase2_policy,
                            &phase2_retention,
                        )
                        .await;
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
        let payload_policy = self.app_config.payload.clone();
        let phase2_policy = self.app_config.payload.clone();
        let phase2_retention = self.app_config.retention.clone();

        // Mark this task in-dispatch until the spawned execution finishes, so the
        // buffered probe won't re-fetch it while it is still CLAIMED (P7).
        let task_id = row.id.to_string();
        self.in_dispatch
            .lock()
            .expect("in_dispatch poisoned")
            .insert(task_id.clone());
        let in_dispatch = Arc::clone(&self.in_dispatch);

        self.tracker.spawn(async move {
            let phase2_work = execution::execute_and_finalize(
                Arc::clone(&broker),
                task_fn,
                row,
                worker_id,
                hostname,
                recovery,
                payload_policy,
            )
            .await;

            // Release permit immediately after Phase 1 completes,
            // before potentially slow Phase 2 workflow callbacks.
            drop(permit);

            if let Some(work) = phase2_work {
                execution::run_phase2(
                    broker.pool(),
                    &workflow_registry,
                    work,
                    &phase2_policy,
                    &phase2_retention,
                )
                .await;
            }

            in_dispatch
                .lock()
                .expect("in_dispatch poisoned")
                .remove(&task_id);
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
                enqueue_sha, command_fingerprint_version, command_fingerprint,
                retention_class_key, retain_rerun_input, prepared_rerun_input_disposition
            ) VALUES (
                $1, 'finalize_test', $2, 100, '[]', '{}', 'CLAIMED',
                NOW(), NOW(), NOW(), NOW(), TRUE,
                'worker-1', $3, $4, $5,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                1, decode(repeat('00', 32), 'hex'), 'standard_30d', FALSE, 'NEVER_ELIGIBLE'
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
                retry_count, max_retries, task_options, enqueue_sha,
                command_fingerprint_version, command_fingerprint,
                retention_class_key, retain_rerun_input, prepared_rerun_input_disposition
            ) VALUES (
                $1, 'finalize_test', $2, 100, '[]', '{}', 'RUNNING',
                NOW(), NOW(), NOW(), NOW(), FALSE,
                $3, $4, $5,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                1, decode(repeat('00', 32), 'hex'), 'standard_30d', FALSE, 'NEVER_ELIGIBLE'
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

    /// Build a dispatch row for a task seeded by `insert_claimed_task` /
    /// `insert_running_task`. Retry counts live on the DB row only: the
    /// execution path reads them from `set_running`'s RETURNING, not the
    /// claimed row (v12 claim shape).
    fn claimed_task_row(
        task_id: &str,
        queue_name: &str,
        task_options: Option<String>,
    ) -> ClaimedTaskRow {
        ClaimedTaskRow {
            id: Uuid::parse_str(task_id).unwrap(),
            task_name: "finalize_test".to_owned(),
            args: Some("[]".to_owned()),
            kwargs: Some("{}".to_owned()),
            queue_name: queue_name.to_owned(),
            is_workflow_task: false,
            task_options,
            claimed_at: None,
        }
    }

    /// Like `claimed_task_row` but marked as a workflow task, mirroring a row
    /// claimed for a task that `link_task_to_workflow` attached to a workflow.
    fn claimed_workflow_task_row(
        task_id: &str,
        queue_name: &str,
        task_options: Option<String>,
    ) -> ClaimedTaskRow {
        ClaimedTaskRow {
            is_workflow_task: true,
            ..claimed_task_row(task_id, queue_name, task_options)
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

        // A task linked into a workflow is a workflow task; mirror production
        // (workflow inserts set is_workflow_task = TRUE) so phase-2 routing,
        // which now reads the carried column, advances the workflow.
        sqlx::query("UPDATE horsies_tasks SET is_workflow_task = TRUE WHERE id = $1")
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
        assert_eq!(config.max_claim_batch, 0, "default is auto-fill");
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

    // -- compute_ordered_queues unit tests (parity with horsies PR #166) --

    #[test]
    fn ordered_queues_cap_only_queue_serviced_under_partial_priority_map() {
        // Priority mode with a partial map: a cap-only queue must be kept and
        // sort into the default bucket (100), not be dropped.
        let queues = vec!["capped".to_owned(), "prio".to_owned(), "neither".to_owned()];
        let mut priorities = std::collections::HashMap::new();
        priorities.insert("prio".to_owned(), 10);
        let mut caps = std::collections::HashMap::new();
        caps.insert("capped".to_owned(), 3);

        let ordered = compute_ordered_queues(&queues, &priorities, &caps);
        assert_eq!(
            ordered,
            vec!["prio".to_owned(), "capped".to_owned()],
            "cap-only queue serviced in bucket 100; unconfigured queue dropped",
        );
    }

    #[test]
    fn ordered_queues_cap_only_config_stays_round_robin() {
        // No priorities configured: all listed queues are serviced in the
        // configured order, capped or not (caps are enforced downstream).
        let queues = vec!["a".to_owned(), "b".to_owned()];
        let priorities = std::collections::HashMap::new();
        let mut caps = std::collections::HashMap::new();
        caps.insert("b".to_owned(), 3);

        let ordered = compute_ordered_queues(&queues, &priorities, &caps);
        assert_eq!(ordered, queues);
    }

    #[test]
    fn ordered_queues_priority_ties_keep_configured_order() {
        let queues = vec!["z".to_owned(), "a".to_owned()];
        let mut priorities = std::collections::HashMap::new();
        priorities.insert("z".to_owned(), 100);
        priorities.insert("a".to_owned(), 100);
        let caps = std::collections::HashMap::new();

        let ordered = compute_ordered_queues(&queues, &priorities, &caps);
        assert_eq!(
            ordered, queues,
            "stable sort preserves configured order on ties"
        );
    }

    // -- compute_claim_lock_keys unit tests (parity with horsies PR #125) --

    #[test]
    fn claim_lock_keys_cluster_cap_uses_single_global_key() {
        let mut max_conc = std::collections::HashMap::new();
        max_conc.insert("a".to_owned(), 5);
        max_conc.insert("b".to_owned(), 5);
        let ordered = vec!["a".to_owned(), "b".to_owned()];
        // Cluster cap set → exactly the global key, regardless of per-queue caps.
        let keys = compute_claim_lock_keys(Some(10), &ordered, &max_conc);
        assert_eq!(keys, vec![advisory_key_global()]);
    }

    #[test]
    fn claim_lock_keys_per_capped_queue_sorted() {
        let mut max_conc = std::collections::HashMap::new();
        max_conc.insert("a".to_owned(), 5);
        max_conc.insert("b".to_owned(), 5);
        let ordered = vec!["a".to_owned(), "b".to_owned()];
        let keys = compute_claim_lock_keys(None, &ordered, &max_conc);

        let mut expected = vec![advisory_key_queue("a"), advisory_key_queue("b")];
        expected.sort_unstable();
        assert_eq!(keys, expected, "one sorted key per capped queue");
        assert!(
            !keys.contains(&advisory_key_global()),
            "queue keys are a separate namespace from the global key",
        );
    }

    #[test]
    fn claim_lock_keys_excludes_uncapped_queue() {
        let mut max_conc = std::collections::HashMap::new();
        max_conc.insert("capped".to_owned(), 5);
        // "uncapped" is served but absent from the concurrency map.
        let ordered = vec!["capped".to_owned(), "uncapped".to_owned()];
        let keys = compute_claim_lock_keys(None, &ordered, &max_conc);
        assert_eq!(keys, vec![advisory_key_queue("capped")]);
    }

    #[test]
    fn claim_lock_keys_empty_when_no_caps() {
        let max_conc = std::collections::HashMap::new();
        let ordered = vec!["a".to_owned()];
        // No cluster cap, no per-queue caps → no locks.
        assert!(compute_claim_lock_keys(None, &ordered, &max_conc).is_empty());
    }

    #[test]
    fn claim_lock_keys_enforced_for_capped_queue_without_priorities() {
        // C7: a per-queue cap is keyed on `queue_max_concurrency`, not on
        // `queue_priorities`. A capped queue served in round-robin mode (no
        // priorities) must still take its advisory lock so the cap is enforced.
        let mut max_conc = std::collections::HashMap::new();
        max_conc.insert("a".to_owned(), 5);
        let ordered = vec!["a".to_owned()];
        assert_eq!(
            compute_claim_lock_keys(None, &ordered, &max_conc),
            vec![advisory_key_queue("a")],
        );
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

    /// A task that stays RUNNING long enough for a test to observe concurrency.
    async fn block_a_while(_: ()) -> Result<String, TaskError> {
        tokio::time::sleep(Duration::from_secs(30)).await;
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
            crate::core::config::payload::PayloadPolicy::default(),
        )
        .await;

        if let Some(work) = phase2_work {
            let registry = WorkflowSpecRegistry::new();
            let policy = crate::core::config::payload::PayloadPolicy::default();
            run_phase2(
                broker.pool(),
                &registry,
                work,
                &policy,
                &crate::core::RetentionConfig::default(),
            )
            .await;
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
            claimed_task_row(&task_id, "default", None),
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

    /// Insert a RUNNING task owned by a specific worker, simulating a task that
    /// was reaper-requeued and re-claimed (then set RUNNING) by another worker.
    async fn insert_running_task_owned_by(pool: &PgPool, task_id: &str, owner: &str) {
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, args, kwargs, status,
                sent_at, started_at, created_at, updated_at, claimed,
                claimed_by_worker_id, retry_count, max_retries, enqueue_sha,
                command_fingerprint_version, command_fingerprint,
                retention_class_key, retain_rerun_input, prepared_rerun_input_disposition
            ) VALUES (
                $1, 'finalize_test', 'default', 100, '[]', '{}', 'RUNNING',
                NOW(), NOW(), NOW(), NOW(), FALSE,
                $2, 0, 3,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                1, decode(repeat('00', 32), 'hex'), 'standard_30d', FALSE, 'NEVER_ELIGIBLE'
            )",
        )
        .bind(task_id)
        .bind(owner)
        .execute(pool)
        .await
        .unwrap();
    }

    /// A stale finalizer (its task was reclaimed and re-run by another worker)
    /// must not clobber the new owner's in-flight RUNNING attempt. The
    /// `status='RUNNING'` guard alone is insufficient — the re-claimed task is
    /// RUNNING again — so the ownership guard (`claimed_by_worker_id = $wid`) is
    /// load-bearing. Parity with horsies PR #101 dc2cbbd6.
    #[tokio::test]
    #[serial]
    async fn finalize_primitives_reject_stale_owner_and_accept_current_owner() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        use crate::broker::terminalization::terminalize;
        use crate::core::lifecycle::{
            PriorLockedRead, TerminalizationCommand, TerminalizationOutcome,
        };

        let complete_as =
            |task_id: &str, worker: &str| TerminalizationCommand::CompleteLockedTask {
                task_id: task_id.to_owned(),
                fence: PriorLockedRead {
                    worker_id: worker.to_owned(),
                },
                result_json: "{}".to_owned(),
            };
        let fail_as = |task_id: &str, worker: &str, reason: Option<&str>| {
            TerminalizationCommand::FailLockedTask {
                task_id: task_id.to_owned(),
                fence: PriorLockedRead {
                    worker_id: worker.to_owned(),
                },
                result_json: "{}".to_owned(),
                error_code: reason.is_none().then(|| "X".to_owned()),
                failed_reason: reason.map(str::to_owned),
            }
        };
        let applied = |outcomes: &[TerminalizationOutcome]| {
            matches!(outcomes, [TerminalizationOutcome::Applied { .. }])
        };

        // complete: stale owner rejected, current owner applies.
        let t_complete = Uuid::new_v4().to_string();
        insert_running_task_owned_by(&pool, &t_complete, "worker-2").await;
        assert!(
            !applied(
                &terminalize(&pool, &complete_as(&t_complete, "worker-1"))
                    .await
                    .unwrap()
            ),
            "stale owner must not complete the re-claimed task"
        );
        assert_eq!(fetch_task_state(&pool, &t_complete).await.0, "RUNNING");
        assert!(
            applied(
                &terminalize(&pool, &complete_as(&t_complete, "worker-2"))
                    .await
                    .unwrap()
            ),
            "current owner must complete its own task"
        );
        assert_eq!(fetch_task_state(&pool, &t_complete).await.0, "COMPLETED");

        // fail: stale owner rejected, current owner applies.
        let t_fail = Uuid::new_v4().to_string();
        insert_running_task_owned_by(&pool, &t_fail, "worker-2").await;
        assert!(
            !applied(
                &terminalize(&pool, &fail_as(&t_fail, "worker-1", None))
                    .await
                    .unwrap()
            ),
            "stale owner must not fail the re-claimed task"
        );
        assert_eq!(fetch_task_state(&pool, &t_fail).await.0, "RUNNING");
        assert!(applied(
            &terminalize(&pool, &fail_as(&t_fail, "worker-2", None))
                .await
                .unwrap()
        ));
        assert_eq!(fetch_task_state(&pool, &t_fail).await.0, "FAILED");

        // requeue: stale owner rejected, current owner applies.
        let t_requeue = Uuid::new_v4().to_string();
        insert_running_task_owned_by(&pool, &t_requeue, "worker-2").await;
        assert!(
            !broker
                .requeue(&t_requeue, Some(Utc::now()), "worker-1")
                .await
                .unwrap(),
            "stale owner must not requeue the re-claimed task"
        );
        assert_eq!(fetch_task_state(&pool, &t_requeue).await.0, "RUNNING");
        assert!(broker
            .requeue(&t_requeue, Some(Utc::now()), "worker-2")
            .await
            .unwrap());
        assert_eq!(fetch_task_state(&pool, &t_requeue).await.0, "PENDING");

        // worker-crash failure (failed_reason carried): stale owner rejected,
        // current owner applies. Same operation as task failure — they differ
        // only in whether a failure reason travels.
        let t_worker = Uuid::new_v4().to_string();
        insert_running_task_owned_by(&pool, &t_worker, "worker-2").await;
        assert!(
            !applied(
                &terminalize(&pool, &fail_as(&t_worker, "worker-1", Some("crash")))
                    .await
                    .unwrap()
            ),
            "stale owner must not worker-fail the re-claimed task"
        );
        assert_eq!(fetch_task_state(&pool, &t_worker).await.0, "RUNNING");
        assert!(applied(
            &terminalize(&pool, &fail_as(&t_worker, "worker-2", Some("crash")))
                .await
                .unwrap()
        ));
        assert_eq!(fetch_task_state(&pool, &t_worker).await.0, "FAILED");
    }

    /// A late task completion must not resurrect a terminal (CANCELLED) workflow.
    /// Rust sets `completed_at` on cancel, so the `completed_at IS NULL` guard
    /// already blocks resurrection; this test forces `completed_at = NULL` to
    /// exercise the `status='RUNNING'` short-circuit independently. Parity with
    /// horsies PR #101 ed61c3a2.
    #[tokio::test]
    #[serial]
    async fn check_completion_does_not_resurrect_cancelled_workflow() {
        let pool = test_pool().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4().to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        let workflow_id = link_task_to_workflow(&pool, &task_id, Some(0)).await;

        // Drive the single node terminal so the non-terminal count is zero.
        sqlx::query("UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = '{}', completed_at = NOW() WHERE workflow_id = $1")
            .bind(&workflow_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE horsies_tasks SET status = 'COMPLETED', result = '{}', terminal_at = NOW() WHERE id = $1")
            .bind(&task_id)
            .execute(&pool)
            .await
            .unwrap();

        // Terminal workflow with completed_at deliberately NULL: only the
        // status guard can prevent resurrection here.
        sqlx::query(
            "UPDATE horsies_workflows SET status = 'CANCELLED', completed_at = NULL WHERE id = $1",
        )
        .bind(&workflow_id)
        .execute(&pool)
        .await
        .unwrap();

        let registry = WorkflowSpecRegistry::new();
        crate::workflow_engine::engine::check_workflow_completion(
            &pool,
            &workflow_id,
            &registry,
            &crate::core::config::payload::PayloadPolicy::default(),
            &crate::core::RetentionConfig::default(),
        )
        .await
        .unwrap();

        let (workflow_status, _) = fetch_workflow_state(&pool, &workflow_id).await;
        assert_eq!(
            workflow_status, "CANCELLED",
            "a late completion must not resurrect a CANCELLED workflow"
        );
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
            claimed_workflow_task_row(&task_id, "default", None),
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
            claimed_workflow_task_row(&task_id, "default", None),
            "worker-1".to_owned(),
            "localhost".to_owned(),
            RecoveryConfig::default(),
            crate::core::config::payload::PayloadPolicy::default(),
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
        let policy = crate::core::config::payload::PayloadPolicy::default();
        run_phase2(
            broker.pool(),
            &registry,
            phase2_work,
            &policy,
            &crate::core::RetentionConfig::default(),
        )
        .await;

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
            claimed_workflow_task_row(&task_id, "default", None),
            "worker-1".to_owned(),
            "localhost".to_owned(),
            RecoveryConfig::default(),
            crate::core::config::payload::PayloadPolicy::default(),
        )
        .await
        .expect("terminal success should produce phase 2 work");

        let failed_phase2_pool = test_pool().await;
        failed_phase2_pool.close().await;

        let registry = WorkflowSpecRegistry::new();
        let policy = crate::core::config::payload::PayloadPolicy::default();
        run_phase2(
            &failed_phase2_pool,
            &registry,
            phase2_work,
            &policy,
            &crate::core::RetentionConfig::default(),
        )
        .await;

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
    async fn finalize_prestart_workflow_update_failure_is_nonfatal_and_runs_user_code() {
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
                claimed_task_row(&task_id, "default", None),
            )
            .await;

            // C4: the workflow_task RUNNING update is purely informational
            // (set_running already committed the CLAIMED->RUNNING flip durably,
            // and completion tolerates an ENQUEUED workflow_task). A transient
            // failure of it must NOT fail the task closed — user code runs and
            // the task completes normally.
            assert_eq!(
                EXECUTION_COUNT.load(Ordering::SeqCst),
                1,
                "user code must still run when the informational workflow_task RUNNING update fails",
            );

            let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
            assert_eq!(status, "COMPLETED");
            assert_eq!(error_code, None);

            let persisted: TaskResult<serde_json::Value> =
                serde_json::from_str(result.as_deref().unwrap()).unwrap();
            assert_eq!(
                persisted.unwrap(),
                serde_json::Value::String("counted".to_owned()),
            );

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
    async fn finalize_paused_workflow_cancels_without_running_user_code() {
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
            claimed_task_row(&task_id, "default", None),
        )
        .await;

        assert_eq!(EXECUTION_COUNT.load(Ordering::SeqCst), 0);

        // Paused-workflow claimed-but-not-started task is moved to terminal
        // CANCELLED (not back to PENDING), so it cannot run as a duplicate of the
        // node after resume. The node is reset to READY for resume to re-enqueue.
        let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "CANCELLED");
        assert_eq!(result, None);
        assert_eq!(error_code.as_deref(), Some("TASK_CANCELLED"));
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
            claimed_task_row(&task_id, "default", None),
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
            claimed_task_row(&task_id, "default", Some(task_options)),
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
            claimed_task_row(&task_id, "default", None),
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
            claimed_task_row(&task_id, "default", None),
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
            &claimed_task_row(&task_id, "default", None),
            &crate::broker::SetRunningRow {
                id: Uuid::parse_str(&task_id).unwrap(),
                started_at: Utc::now(),
                retry_count: 0,
                max_retries: 0,
                good_until: None,
            },
            "worker-1",
            "localhost",
            &crate::core::config::payload::PayloadPolicy::default(),
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
            claimed_task_row(&task_id, "default", None),
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

    /// Capacity signals go only to the per-queue channel; workers no longer
    /// listen on task_new (parity with horsies PR #101 cafc9200).
    #[tokio::test]
    #[serial]
    async fn notify_worker_capacity_emits_queue_specific_signal_only() {
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
        assert_eq!(
            (first.channel().to_owned(), first.payload().to_owned()),
            (
                "task_queue_high-priority".to_owned(),
                "capacity:task-123".to_owned(),
            ),
        );

        // No second signal: task_new must not be emitted.
        let second = tokio::time::timeout(Duration::from_millis(500), listener.recv()).await;
        assert!(
            second.is_err(),
            "capacity signal must not be emitted on task_new"
        );
    }

    #[tokio::test]
    #[serial]
    async fn notify_worker_capacity_swallow_pool_errors() {
        let pool = test_pool().await;
        pool.close().await;
        notify_worker_capacity(&pool, "default", "task-closed").await;
    }

    /// C11: `run()` must fire the cancellation token on a fatal exit path, not
    /// only via signals / `request_stop`. Otherwise the listener, claimer
    /// heartbeat, reaper, workflow-recovery and worker-state loops keep running
    /// after the worker gives up — advertising a dead worker as alive and holding
    /// claim leases. The broker is built with a distinct session URL so the
    /// listener has its own pool: closing only the main pool lets the loop start
    /// (background loops spawn), then makes every claim fail with a retryable
    /// `PoolClosed`; a 1-retry budget exhausts and breaks the loop fatally.
    #[tokio::test]
    #[serial]
    async fn run_fires_cancel_on_fatal_claim_exit() {
        use crate::core::config::{PostgresConfig, QueueMode, WorkerResilienceConfig};

        // Distinct session URL (same DB, extra query param) so the session pool
        // is separate from the main pool.
        let main_url = test_db_url();
        let sep = if main_url.contains('?') { '&' } else { '?' };
        let session_url = format!("{main_url}{sep}application_name=c11_session");
        let pg_config = PostgresConfig {
            database_url: main_url,
            session_database_url: Some(session_url),
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            pool_size: 30,
            max_overflow: 30,
            retain_rerun_input_default: false,
            pool_timeout: 30,
            pool_recycle: 1800,
            echo: false,
        };
        let broker = Arc::new(
            PostgresBroker::connect_with(&pg_config)
                .await
                .expect("connect broker with separate session pool"),
        );

        let app_config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: pg_config,
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: crate::core::RetentionConfig::default(),
            // Exhaust the claim retry budget quickly so the fatal break fires fast.
            resilience: WorkerResilienceConfig {
                db_retry_initial_ms: 100,
                db_retry_max_ms: 100,
                db_retry_max_attempts: 1,
                notify_poll_interval_ms: 100,
            },
            schedule: None,
            resend_on_transient_err: false,
        };

        let worker = Worker::new(
            Arc::clone(&broker),
            Arc::new(TaskRegistry::new()),
            Arc::new(WorkflowSpecRegistry::new()),
            app_config,
            WorkerConfig::default(),
        )
        .expect("worker");
        let cancel = worker.cancel_token();
        assert!(!cancel.is_cancelled(), "token must start uncancelled");

        // Close only the main pool; the listener connects via the session pool.
        broker.pool().close().await;

        let handle = tokio::spawn(async move { worker.run().await });
        let result = tokio::time::timeout(Duration::from_secs(20), handle)
            .await
            .expect("run() must exit after the fatal claim error, not hang")
            .expect("run task join");
        assert!(
            result.is_err(),
            "run() must return the fatal claim error so the process exits \
             non-zero for a supervisor restart: {result:?}",
        );

        assert!(
            cancel.is_cancelled(),
            "run() must cancel the token on a fatal exit so background loops stop (C11)",
        );
    }

    /// C17: in soft-cap mode, buffered-dispatch must not start more of a capped
    /// queue's tasks than its `max_concurrency`. The claim function counts only
    /// RUNNING against the cap, so a worker can accumulate more CLAIMED tasks of
    /// a capped queue than its cap while none run; dispatching all of them would
    /// push queue RUNNING past the cap by up to `prefetch_buffer`.
    #[tokio::test]
    #[serial]
    async fn buffered_dispatch_respects_queue_cap_in_soft_mode() {
        use crate::core::config::{PostgresConfig, QueueMode, WorkerResilienceConfig};

        let broker = test_broker().await;
        let pool = broker.pool().clone();
        clean(&pool).await;

        let mut registry = TaskRegistry::new();
        registry
            .register("block_a_while", async_task_fn!(block_a_while, ()))
            .expect("register");

        let app_config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: test_db_url(),
                session_database_url: None,
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                pool_size: 30,
                max_overflow: 30,
                retain_rerun_input_default: false,
                pool_timeout: 30,
                pool_recycle: 1800,
                echo: false,
            },
            cluster_wide_cap: None,
            prefetch_buffer: 4, // soft-cap mode
            claim_lease_ms: Some(60_000),
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: crate::core::RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        };

        let mut queue_max_concurrency = std::collections::HashMap::new();
        queue_max_concurrency.insert("c17q".to_owned(), 2u32);
        let worker_config = WorkerConfig {
            queues: vec!["c17q".to_owned()],
            concurrency: 10, // ample permits — the cap, not permits, must bind
            queue_max_concurrency,
            ..WorkerConfig::default()
        };

        let worker = Worker::new(
            Arc::clone(&broker),
            Arc::new(registry),
            Arc::new(WorkflowSpecRegistry::new()),
            app_config,
            worker_config,
        )
        .expect("worker");

        // Seed 6 buffered CLAIMED tasks of the capped queue owned by this worker,
        // none RUNNING — the pathological state the claim function permits.
        for _ in 0..6 {
            let id = Uuid::new_v4().to_string();
            sqlx::query(
                "INSERT INTO horsies_tasks (
                    id, task_name, queue_name, priority, args, kwargs, status,
                    sent_at, enqueued_at, claimed_at, created_at, updated_at, claimed,
                    claimed_by_worker_id, claim_expires_at, retry_count, max_retries,
                    is_workflow_task, enqueue_sha, command_fingerprint_version,
                    command_fingerprint, retention_class_key, retain_rerun_input,
                    prepared_rerun_input_disposition
                ) VALUES (
                    $1, 'block_a_while', 'c17q', 100, '[]', '{}', 'CLAIMED',
                    NOW(), NOW(), NOW(), NOW(), NOW(), TRUE,
                    $2, NOW() + INTERVAL '60 seconds', 0, 3,
                    FALSE, '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                    1, decode(repeat('00', 32), 'hex'), 'standard_30d', FALSE, 'NEVER_ELIGIBLE'
                )",
            )
            .bind(&id)
            .bind(&worker.worker_id)
            .execute(&pool)
            .await
            .unwrap();
        }

        worker
            .claim_and_dispatch_all()
            .await
            .expect("claim_and_dispatch_all");

        // Let the dispatched tasks transition CLAIMED -> RUNNING.
        tokio::time::sleep(Duration::from_millis(800)).await;

        let running: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_tasks WHERE queue_name = 'c17q' AND status = 'RUNNING'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            running <= 2,
            "queue RUNNING ({running}) must not exceed max_concurrency (2) via buffered dispatch",
        );
        assert_eq!(
            running, 2,
            "the cap's worth of buffered tasks must dispatch"
        );

        clean(&pool).await;
    }
}

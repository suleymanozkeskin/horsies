//! The worker: claims rows, runs each task in a child process of the
//! executor, and writes how each attempt ended (Python
//! `core/worker/worker.py`, `Worker.start`, `run_forever`, `stop`).
//!
//! User code never runs in the worker process. The worker starts the
//! children of its profile before any database work, then claims with the
//! budgets of the children in service, submits each row to the pool of its
//! kind, and settles each end with the outcome writer. In soft-cap mode a
//! row of a capped queue waits in the parent until its queue's count leaves
//! room (`queue_cap_hold`).

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::Utc;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::broker::claims::{ClaimClasses, ClaimPassRequest, ClaimedRow};
use crate::broker::{NotifyListener, PostgresBroker};
use crate::core::config::app::AppConfig;
use crate::core::config::worker_profile::TaskLimitSetting;
use crate::core::history::maintenance::coverage::{
    ensure_startup_coverage_in_pool, StartupCoverageOutcome,
};
use crate::core::history::reads::publisher::StagedLoaderPublisher;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::db::Reserve;
use crate::worker::backoff::RetryBackoff;
use crate::worker::children::{
    attempt_end, baseline_limits, exit_not_seen, pending_task_limit, plan_placement, ChildLaunch,
    ChildrenStopped, PoolFacts, WorkerChildren,
};
use crate::worker::claim_pass::{
    claim_for_dispatch, BrokerClaimSteps, ClaimBatchLimit, ClaimPassCounts, RegisteredClasses,
};
use crate::worker::config::WorkerConfig;
use crate::worker::error::WorkerError;
use crate::worker::executor::placement::WorkerThreads;
use crate::worker::executor::protocol::TaskInputText;
use crate::worker::executor::slots::{
    describe_claim_plan, guard_new_kind_baselines, start_children, unserved_classes_line,
    AttemptEnd, BaselineOverLimit, ChildrenStart, ChildrenStartFailed, Claimed, ClaimedTask,
    NotRunCause, OrphanSelfHeal, OutcomeWriter, PoolBoard, PoolFailure, PoolStopReport, RetryFacts,
    TaskKind,
};
use crate::worker::executor::supervisor::{Spawner, SpawnerStopReport, CHILD_KILL_WAIT};
use crate::worker::finalize_batch::statement::WorkerPoolSuccessWrite;
use crate::worker::finalize_batch::stop::drain_then_close;
use crate::worker::finalize_batch::{FinalizeBatcher, FlusherBound};
use crate::worker::heartbeat::spawn_claimer_heartbeat;
use crate::worker::profile_fit::{
    log_connection_fit_at_boot, log_memory_fit_at_boot, SelectedProfile,
};
use crate::worker::queue_cap_hold::{admit_held_rows, QueueCapHold, StartCheckCaps, StartCount};
use crate::worker::recovery::{new_reaper_health, spawn_reaper};
use crate::worker::retry::read_stored_options;
use crate::worker::runner_beats::{
    launch_runner_beats, runner_beat_pool, BeatSender, FailClosedState, FailClosedStop,
    LiveDispatches, RunnerBeatWatch, StaleRunningRecovery,
};
use crate::worker::worker_state::ClaimColumns;

/// How long the stop waits for the outcome writes in flight after the pools
/// stopped (Python `_FINALIZER_DRAIN_TIMEOUT_S`). The children have ended,
/// so this bounds database work, not task run time.
pub const OUTCOME_WRITE_DRAIN: Duration = Duration::from_secs(30);

/// How long the stop waits for the spawner thread to reap every child.
pub const SPAWNER_STOP_WAIT: Duration = CHILD_KILL_WAIT;

/// The error text of a failed start hook (Python `_HOOK_FAILURE_MESSAGE`).
pub const CHILD_HOOK_FAILURE_MESSAGE: &str = "on_child_process_start hook failed in a worker \
     child (see the child log line above for the hook name); fix the hook, the worker will not \
     restart-loop on it";

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
/// The lease of every claim: `claim_lease_ms`, or [`DEFAULT_CLAIM_LEASE_MS`]
/// when it is not set. Every claim is lease-bounded, in hard-cap and soft-cap
/// mode alike, so a crashed worker's claims become reclaimable (Python
/// `claiming.py:697-708`).
fn effective_claim_lease_ms(app_config: &AppConfig) -> u32 {
    app_config
        .claim_lease_ms
        .unwrap_or(crate::core::defaults::DEFAULT_CLAIM_LEASE_MS)
}

/// The lease of a claim pass. The config refuses a zero `claim_lease_ms` when
/// it is loaded, and the default is positive, so zero here is a defect.
fn claim_lease(app_config: &AppConfig) -> std::num::NonZeroU32 {
    match std::num::NonZeroU32::new(effective_claim_lease_ms(app_config)) {
        Some(lease) => lease,
        None => unreachable!("claim_lease_ms of 0 is refused when the config is loaded"),
    }
}

/// Bound of one attempt of the database start steps (Python
/// `_start_with_resilience_config`: `asyncio.wait_for(self.start(), 30.0)`).
const START_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

/// How the worker start ended without an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartOutcome {
    /// The database steps completed; the worker goes on to listen and claim.
    Started,
    /// A stop was requested while a retry waited; the worker does not start.
    StopRequested,
}

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

#[derive(Debug, PartialEq, Eq)]
enum ClaimWake {
    Cancelled,
    CapacityReleased,
    DatabaseNotification,
    ListenerClosed,
    PollElapsed,
}

async fn wait_for_claim_wake(
    cancel: &CancellationToken,
    capacity_released: &Notify,
    notifications: &mut mpsc::Receiver<sqlx::postgres::PgNotification>,
    poll_interval: Duration,
) -> ClaimWake {
    tokio::select! {
        _ = cancel.cancelled() => ClaimWake::Cancelled,
        _ = capacity_released.notified() => ClaimWake::CapacityReleased,
        notification = notifications.recv() => match notification {
            Some(_) => ClaimWake::DatabaseNotification,
            None => ClaimWake::ListenerClosed,
        },
        _ = tokio::time::sleep(poll_interval) => ClaimWake::PollElapsed,
    }
}

/// Why the children stopped the worker while it ran (Python
/// `_slot_failure_loop`, `_stop_for_baseline`).
#[derive(Debug, Clone, PartialEq)]
enum ChildrenStop {
    /// A start hook failed in a replacement child: a graceful stop, with no
    /// restart loop (Python `_stop_for_child_hook_failure`).
    HookFailed,
    /// No replacement child can start: the worker exits non-zero.
    PoolFailed(PoolFailure),
    /// The first child of a kind that was down at the first start has a
    /// baseline at or above the memory limit of its kind.
    BaselineOverLimit(BaselineOverLimit),
}

/// The first reason the children gave to stop the worker.
#[derive(Clone, Default)]
struct ChildrenStopCell(Arc<Mutex<Option<ChildrenStop>>>);

impl ChildrenStopCell {
    /// Keep `stop` when no reason is kept yet, then stop the worker.
    fn stop_worker(&self, stop: ChildrenStop, cancel: &CancellationToken) {
        {
            let mut kept = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            if kept.is_none() {
                *kept = Some(stop);
            }
        }
        cancel.cancel();
    }

    fn take(&self) -> Option<ChildrenStop> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

/// End the worker when no replacement child can start (Python
/// `_slot_failure_loop`). A failed start hook stops it gracefully; any other
/// failure makes the worker exit non-zero. Returns at the stop.
async fn watch_pool_failure(
    board: Arc<PoolBoard>,
    stops: ChildrenStopCell,
    cancel: CancellationToken,
) {
    tokio::select! {
        () = cancel.cancelled() => {}
        failure = board.wait_failed() => {
            let stop = match failure {
                PoolFailure::ChildHookFailed => {
                    tracing::error!("Stopping worker: {CHILD_HOOK_FAILURE_MESSAGE}");
                    ChildrenStop::HookFailed
                }
                failure @ (PoolFailure::BootAttemptsExhausted { .. }
                | PoolFailure::SlotDefectsRepeated { .. }) => {
                    tracing::error!(
                        "Background task 'slot-failure-watch' hit a process-fatal error; \
                         stopping worker: worker child start failed: {failure}"
                    );
                    ChildrenStop::PoolFailed(failure)
                }
            };
            stops.stop_worker(stop, &cancel);
        }
    }
}

/// Run the memory baseline guard on the first child of each kind that was
/// down at the first start (Python `_new_kind_baseline_loop`). A breach stops
/// the worker, which exits non-zero after the drain. Returns at the stop.
async fn watch_new_kind_baselines(
    board: Arc<PoolBoard>,
    limits: crate::worker::executor::slots::BaselineLimits,
    stops: ChildrenStopCell,
    cancel: CancellationToken,
) {
    tokio::select! {
        () = cancel.cancelled() => {}
        over = guard_new_kind_baselines(&board, &limits) => {
            stops.stop_worker(ChildrenStop::BaselineOverLimit(over), &cancel);
        }
    }
}

/// The children of a started worker, the spawner thread that starts them,
/// and the registry of the tasks that children hold.
struct RunningChildren {
    children: WorkerChildren,
    spawner: Spawner,
    live: LiveDispatches,
}

impl RunningChildren {
    /// Stop the pools with `drain` (Python `SlotPool.stop`), then the
    /// spawner thread. Logs a drain that had to kill children.
    async fn stop_pools(children: WorkerChildren, drain: Duration) {
        let ChildrenStopped { report } = children.stop(drain).await;
        match report {
            PoolStopReport::Drained => {}
            PoolStopReport::KilledAtDrainDeadline | PoolStopReport::AbortedAfterDrain => {
                tracing::error!(
                    "Drain exceeded {:.0}s (hung task?); the children that still ran tasks were \
                     killed",
                    drain.as_secs_f64()
                );
            }
        }
        tracing::info!("Worker child processes stopped");
    }

    /// Stop the spawner thread after the pools stopped. Never raises.
    async fn stop_spawner(spawner: Spawner) {
        match tokio::task::spawn_blocking(move || spawner.stop(SPAWNER_STOP_WAIT)).await {
            Ok(SpawnerStopReport::AllReaped) => {}
            Ok(SpawnerStopReport::BoundPassed) => tracing::warn!(
                "the spawner thread did not reap every child within {:.0}s",
                SPAWNER_STOP_WAIT.as_secs_f64()
            ),
            Err(error) => tracing::error!(error = %error, "stopping the spawner thread failed"),
        }
    }

    /// Stop a start that did not reach the claim loop: the pools at once,
    /// then the spawner (Python `_cleanup_after_failed_start`,
    /// `pool.stop(0)`).
    async fn stop_after_failed_start(self) {
        Self::stop_pools(self.children, Duration::ZERO).await;
        Self::stop_spawner(self.spawner).await;
    }
}

/// The service loops of a serving worker, as the stop needs them.
struct Services {
    /// The deaths of the service loops that no stop requested.
    fatal_rx: mpsc::UnboundedReceiver<crate::worker::supervision::ServiceExit>,
    /// Stops the runner heartbeat writer, after the outcome writes.
    beats_cancel: CancellationToken,
    runner_beat_watch: RunnerBeatWatch,
    fail_closed_stop: FailClosedStop,
}

/// The service loops after the runner heartbeats stopped.
struct ServicesStopped {
    fatal_rx: mpsc::UnboundedReceiver<crate::worker::supervision::ServiceExit>,
    fail_closed_stop: FailClosedStop,
}

impl Services {
    /// Stop the runner heartbeat writer and its watchdog thread (Python
    /// `_stop_runner_heartbeat`, the last step of the stop). Never raises.
    async fn stop_runner_beats(self) -> ServicesStopped {
        self.beats_cancel.cancel();
        let watch = self.runner_beat_watch;
        if tokio::task::spawn_blocking(move || watch.stop())
            .await
            .is_err()
        {
            tracing::error!("stopping the runner heartbeat watchdog failed");
        }
        ServicesStopped {
            fatal_rx: self.fatal_rx,
            fail_closed_stop: self.fail_closed_stop,
        }
    }
}

/// The claimed task and its input, from a claimed row. The stored options
/// are read once, here.
fn claimed_of(row: ClaimedRow) -> Claimed {
    let kind = match row.is_workflow_task {
        true => TaskKind::WorkflowTask,
        false => TaskKind::Plain,
    };
    Claimed {
        task: ClaimedTask {
            task_id: row.id,
            task_name: row.task_name,
            queue_name: row.queue_name,
            claimed_at: row.claimed_at,
            execution: row.execution,
            kind,
            retry: RetryFacts {
                retry_count: row.retry_count,
                max_retries: row.max_retries,
            },
            options: read_stored_options(row.task_options.as_deref()),
        },
        input: TaskInputText::new(row.input),
    }
}

/// Return each row that waited for its queue cap to the queue, at the stop
/// (Python `_return_held_rows`). Runs after the claim loop ended and the
/// pools stopped. Each row is one release write of the outcome writer
/// (`NotDispatched(HeldForQueueCap)`), spawned with the outcome writes, so
/// the drain of the writes bounds it. A row the release cannot return goes
/// back when its lease expires.
fn return_held_rows(
    hold: &mut QueueCapHold,
    writer: &Arc<OutcomeWriter>,
    writes: &mut JoinSet<()>,
) {
    for row in hold.drain() {
        let task = claimed_of(row).task;
        let writer = Arc::clone(writer);
        writes.spawn(async move {
            let settled = writer
                .settle(
                    &task,
                    AttemptEnd::NotDispatched(NotRunCause::HeldForQueueCap),
                )
                .await;
            tracing::debug!(task_id = %task.task_id, settlement = ?settled, "held row returned");
        });
    }
}

/// Task queue worker.
///
/// Claims tasks from PostgreSQL with the budgets of its children, runs each
/// in a child process of the executor, and writes how each attempt ended.
pub struct Worker {
    broker: Arc<PostgresBroker>,
    workflow_registry: Arc<WorkflowSpecRegistry>,
    app_config: AppConfig,
    worker_config: WorkerConfig,
    /// The profile the worker runs: its children, claim limits, drain
    /// timeout and CPU placement.
    profile: SelectedProfile,
    /// What every child runs.
    launch: ChildLaunch,
    /// The class of each task the host registered, for the row class check.
    registered: RegisteredClasses,
    worker_id: String,
    /// Woken when an outcome write ends: a slot or seat is free again.
    capacity_released: Arc<Notify>,
    cancel: CancellationToken,
    hostname: String,
}

impl Worker {
    /// Create a worker that runs `profile`. `launch` names what every child
    /// runs; `registered` the class of each task of the app.
    ///
    /// Errors: [`WorkerError::Config`] for a worker config that does not
    /// validate, or a profile that starts no child (AppConfig validation
    /// refuses such a profile; Python `execution_settings`).
    pub fn new(
        broker: Arc<PostgresBroker>,
        workflow_registry: Arc<WorkflowSpecRegistry>,
        app_config: AppConfig,
        worker_config: WorkerConfig,
        profile: SelectedProfile,
        launch: ChildLaunch,
        registered: RegisteredClasses,
    ) -> Result<Self, WorkerError> {
        worker_config.validate().map_err(WorkerError::Config)?;
        if profile.profile.child_count() == 0 {
            return Err(WorkerError::Config(format!(
                "{}: the profile starts no children; a worker needs at least one child to run \
                 a task",
                crate::core::config::worker_profile::profile_path(&profile.name)
            )));
        }

        let hostname = gethostname::gethostname()
            .into_string()
            .unwrap_or_else(|_| "unknown".to_owned());

        Ok(Self {
            capacity_released: Arc::new(Notify::new()),
            broker,
            workflow_registry,
            app_config,
            worker_config,
            profile,
            launch,
            registered,
            worker_id: Uuid::new_v4().to_string(),
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

    /// The facts that every pool of this worker shares.
    fn pool_facts(&self, placement: crate::worker::executor::placement::CpuPlacement) -> PoolFacts {
        PoolFacts {
            worker_id: self.worker_id.clone(),
            pending_limit: pending_task_limit(
                &self.profile.profile,
                self.app_config.prefetch_buffer,
            ),
            exit_not_seen: exit_not_seen(&self.app_config.recovery),
            placement,
        }
    }

    /// Start the children before any database work, so a bad app or hook
    /// fails at once (Python `start`, `_start_slots`): the CPU plan, the
    /// spawner thread, one pool per kind, then the wait for the first
    /// generation, the placement of the worker's threads and the memory
    /// baseline guard. A stop during the start ends it with `Ok(None)`.
    ///
    /// Errors: [`WorkerError::PlacementRefused`], [`WorkerError::SpawnerStart`],
    /// [`WorkerError::ChildHookFailed`] (never retried),
    /// [`WorkerError::ChildrenStartFailed`]. The pools and the spawner are
    /// stopped before an error returns.
    async fn start_pools(&self) -> Result<Option<RunningChildren>, WorkerError> {
        let profile = &self.profile.profile;
        let placement =
            plan_placement(profile.parent_cpu).map_err(WorkerError::PlacementRefused)?;
        // The spawner thread starts from a blocking thread, not from a task
        // of the core runtime (`Spawner::start`).
        let spawner = match tokio::task::spawn_blocking(Spawner::start).await {
            Ok(started) => started.map_err(WorkerError::SpawnerStart)?,
            Err(joined) => std::panic::resume_unwind(joined.into_panic()),
        };
        let live = LiveDispatches::default();
        let children = WorkerChildren::start(
            profile,
            &self.launch,
            &self.pool_facts(placement.clone()),
            &spawner.handle(),
            &live,
        );
        let running = RunningChildren {
            children,
            spawner,
            live,
        };
        let limits = baseline_limits(profile);
        let threads = WorkerThreads::of_this_process();
        let board = Arc::clone(running.children.board());
        let start = start_children(&board, &placement, &threads, &limits);
        tokio::pin!(start);
        let started = tokio::select! {
            started = &mut start => started,
            () = self.cancel.cancelled() => {
                board.begin_stop();
                start.await
            }
        };
        match started {
            Ok(ChildrenStart::Started { .. }) => Ok(Some(running)),
            Ok(ChildrenStart::Stopped) => {
                running.stop_after_failed_start().await;
                Ok(None)
            }
            Err(ChildrenStartFailed::Pool(PoolFailure::ChildHookFailed)) => {
                running.stop_after_failed_start().await;
                Err(WorkerError::ChildHookFailed)
            }
            Err(failed) => {
                running.stop_after_failed_start().await;
                Err(WorkerError::ChildrenStartFailed(failed))
            }
        }
    }

    /// Authorize the schema and ensure the partition coverage of the
    /// current period: the database steps of the worker start. It does not
    /// retry.
    async fn start_database(&self) -> Result<(), WorkerError> {
        self.broker.ensure_schema_initialized().await?;
        let declared_periods = self.app_config.retention.registrable_periods();
        let startup_coverage = ensure_startup_coverage_in_pool(
            self.broker.session_pool(),
            self.app_config.retention.history_leaf_horizon_days,
            self.app_config.retention.heartbeat_leaf_horizon_hours,
            &declared_periods,
            &StagedLoaderPublisher,
        )
        .await?;
        match startup_coverage {
            StartupCoverageOutcome::Ready(outcome) => {
                tracing::info!(outcome = ?outcome, "task-history startup coverage ready");
            }
            StartupCoverageOutcome::Refused(outcome) => {
                return Err(WorkerError::Config(format!(
                    "task-history startup coverage refused: {outcome:?}"
                )));
            }
        }
        Ok(())
    }

    /// Run [`Worker::start_database`] with each attempt bounded at
    /// [`START_ATTEMPT_TIMEOUT`]. A timed-out attempt and a retryable error
    /// are retried with the resilience backoff; a refusal, or a retryable
    /// error after the retry budget, is returned. A stop request during a
    /// backoff wait ends the start without an error.
    async fn start_database_with_resilience(&self) -> Result<StartOutcome, WorkerError> {
        let mut backoff = RetryBackoff::from_config(&self.app_config.resilience);
        loop {
            let error =
                match tokio::time::timeout(START_ATTEMPT_TIMEOUT, self.start_database()).await {
                    Ok(Ok(())) => return Ok(StartOutcome::Started),
                    Ok(Err(error)) if !error.is_retryable() => return Err(error),
                    Ok(Err(error)) => error,
                    Err(_elapsed) => WorkerError::StartTimedOut {
                        bound: START_ATTEMPT_TIMEOUT,
                    },
                };
            if !backoff.can_retry() {
                tracing::error!(
                    %error,
                    attempts = backoff.attempts(),
                    "worker start failed and its retries are exhausted",
                );
                return Err(error);
            }
            let delay = backoff.next_delay_seconds();
            tracing::error!(
                %error,
                attempt = backoff.attempts(),
                delay_s = format!("{:.1}", delay),
                "worker start failed; retrying",
            );
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(StartOutcome::StopRequested),
                _ = tokio::time::sleep(Duration::from_secs_f64(delay)) => {}
            }
        }
    }

    /// Run the worker until it is stopped or a fatal error occurs (Python
    /// `run_forever`).
    ///
    /// Order: the memory fit of the profile (a log line), the children and
    /// their first generation, the database start (retried with backoff), the
    /// connection fit (a log line), then the listener, the service loops and
    /// the claim loop. The stop runs in Python's order: the claim loop and
    /// the service loops, the pools with the profile's drain timeout, the
    /// outcome writes and the finalize batcher, the runner heartbeats last,
    /// then the spawner thread.
    pub async fn run(&self) -> Result<(), WorkerError> {
        // Machine facts of the profile: a warning, never a refusal (Python
        // `_log_memory_fit`, before the schema step).
        let _logged = log_memory_fit_at_boot(&self.profile);
        let Some(running) = self.start_pools().await? else {
            return Ok(());
        };
        // Fleet authorization and partition coverage precede every runtime
        // surface: no LISTEN, heartbeat, or claim may race an incomplete
        // install or a missing current-hour heartbeat leaf. A transient
        // failure or a slow attempt is retried with backoff (Python
        // `_start_with_resilience_config`).
        match self.start_database_with_resilience().await {
            Ok(StartOutcome::Started) => {}
            Ok(StartOutcome::StopRequested) => {
                running.stop_after_failed_start().await;
                return Ok(());
            }
            Err(error) => {
                running.stop_after_failed_start().await;
                return Err(error);
            }
        }
        // Server facts of the profile, before any claim: a warning, never a
        // refusal (Python `_log_connection_fit`).
        let _logged =
            log_connection_fit_at_boot(&self.profile, &self.app_config.broker, &self.broker).await;
        let batcher = match self.finalize_batcher() {
            Ok(batcher) => batcher,
            Err(error) => {
                running.stop_after_failed_start().await;
                return Err(error);
            }
        };
        self.serve(running, batcher).await
    }

    /// The finalize batcher of the plain successes, on the worker pool of
    /// the broker: as many flushers as its batch connections leave after the
    /// claim pass, and a queue bound of the pending limit.
    ///
    /// Errors: [`WorkerError::Config`] when the broker's main pool keeps no
    /// reserve (a broker not built with `connect_worker_with`), or leaves no
    /// connection for a flusher.
    fn finalize_batcher(&self) -> Result<FinalizeBatcher<WorkerPoolSuccessWrite>, WorkerError> {
        let sizing = self.broker.core_main_pool().sizing();
        match sizing.reserve() {
            Reserve::KeepFree(_) => {}
            Reserve::NoReserve => {
                return Err(WorkerError::Config(
                    "the broker of a worker needs the worker pool, which keeps connections free \
                     for the per-task writes; connect it with PostgresBroker::connect_worker_with"
                        .to_owned(),
                ))
            }
        }
        let flushers = FlusherBound::of_worker_pool(&sizing)
            .map_err(|refused| WorkerError::Config(refused.to_string()))?;
        Ok(FinalizeBatcher::new(
            WorkerPoolSuccessWrite::new(Arc::clone(&self.broker), self.worker_id.clone()),
            pending_task_limit(&self.profile.profile, self.app_config.prefetch_buffer),
            flushers,
        ))
    }

    /// Log the settings of the claim (Python `start`: the concurrency line
    /// and `_log_claim_plan`).
    fn log_claim_settings(&self, children: &WorkerChildren) {
        let profile = &self.profile.profile;
        let claim = ClaimColumns::of_profile(profile);
        tracing::info!(
            "Concurrency config: processes={}, shared_async_children={}, \
             shared_thread_children={}, seats={}, cluster_wide_cap={}, max_claim_per_worker={}, \
             max_claim_batch={}",
            profile.isolated.child_count(),
            profile.shared_async.child_count(),
            profile.shared_thread.child_count(),
            profile.seat_count(),
            self.app_config
                .cluster_wide_cap
                .map_or_else(|| "unlimited".to_owned(), |cap| cap.to_string()),
            claim.max_claim_per_worker,
            claim.max_claim_batch,
        );
        let plan = children.service().plan();
        tracing::info!("Claim plan: {}", describe_claim_plan(plan));
        if let Some(line) = unserved_classes_line(plan) {
            tracing::info!("{line}");
        }
    }

    /// The claim limit of a task count of the profile; no limit is 0, the
    /// value the claim function reads as none.
    fn claim_limit(setting: TaskLimitSetting) -> u32 {
        match setting {
            TaskLimitSetting::TaskLimit { tasks } => tasks.get(),
            TaskLimitSetting::NoTaskLimit {} => 0,
        }
    }

    /// Whether `max_claim_batch` caps a pass.
    fn claim_batch_limit(&self) -> ClaimBatchLimit {
        match self.profile.profile.claim.per_queue_per_pass {
            TaskLimitSetting::TaskLimit { .. } => ClaimBatchLimit::CapsThePass,
            TaskLimitSetting::NoTaskLimit {} => ClaimBatchLimit::NoCap,
        }
    }

    /// The arguments of one claim pass with `classes`, the budgets and
    /// routes of the slots now (Python `_claim_params`).
    fn claim_request(&self, classes: ClaimClasses) -> ClaimPassRequest {
        let ordered = self.ordered_queues();
        let queue_priority: std::collections::HashMap<String, i32> = ordered
            .iter()
            .map(|queue| {
                let priority = self
                    .worker_config
                    .queue_priorities
                    .get(queue)
                    .copied()
                    .unwrap_or(100);
                (queue.clone(), priority)
            })
            .collect();
        let queue_max_concurrency: std::collections::HashMap<String, u32> = ordered
            .iter()
            .filter_map(|queue| {
                self.worker_config
                    .queue_max_concurrency
                    .get(queue)
                    .map(|&cap| (queue.clone(), cap))
            })
            .collect();
        let claim = &self.profile.profile.claim;
        ClaimPassRequest {
            worker_id: self.worker_id.clone(),
            lock_keys: self.claim_lock_keys(&ordered),
            queues: ordered,
            queue_priority,
            queue_max_concurrency,
            hard_cap_mode: self.app_config.prefetch_buffer == 0,
            classes,
            prefetch_buffer: self.app_config.prefetch_buffer,
            max_claim_per_worker: Self::claim_limit(claim.claimed_per_worker),
            max_claim_batch: Self::claim_limit(claim.per_queue_per_pass),
            cluster_wide_cap: self.app_config.cluster_wide_cap,
            claim_lease_ms: claim_lease(&self.app_config),
        }
    }

    /// One claim pass (Python `_claim_and_dispatch_all`): the claim with the
    /// budgets and routes of the slots now, the workflow filter, the row
    /// class check, then each row to the pool of its kind. A row of a queue
    /// that `hold` checks waits in `hold`; then the start check admits the
    /// held rows that their queue's count leaves room for, also on a pass
    /// that claimed nothing.
    ///
    /// Errors: the error of the claim, the filter, the class return or the
    /// start check. The rows the claim committed before a later step failed
    /// go back when their lease expires; the rows submitted before the start
    /// check failed run, and the held rows stay held.
    async fn claim_and_dispatch(
        &self,
        steps: &BrokerClaimSteps,
        children: &WorkerChildren,
        writer: &Arc<OutcomeWriter>,
        writes: &mut JoinSet<()>,
        hold: &mut QueueCapHold,
    ) -> Result<ClaimPassCounts, WorkerError> {
        let request = self.claim_request(children.service().claim_classes());
        let claimed = claim_for_dispatch(steps, &request, &self.registered).await?;
        for row in claimed.rows {
            match hold.holds(&row.queue_name) {
                true => hold.hold(row),
                false => self.dispatch(
                    children,
                    writer,
                    writes,
                    claimed_of(row),
                    StartCount::Unchecked,
                ),
            }
        }
        let admitted = admit_held_rows(
            self.broker.core_main_pool(),
            self.broker.statement_log(),
            &self.worker_id,
            hold,
        )
        .await?;
        for admitted in admitted {
            self.dispatch(
                children,
                writer,
                writes,
                claimed_of(admitted.row),
                StartCount::Admitted(admitted.started),
            );
        }
        Ok(claimed.counts)
    }

    /// Submit one claimed task to the pool of its kind, and spawn the write
    /// of its end (Python `_dispatch_one`, `_finalize_after`). When the write
    /// ends, the claim loop is woken: a slot or seat is free again (Python
    /// `_capacity_released`). An admitted row of a capped queue counts in
    /// its queue's start check until the write ends (`count`). Never fails
    /// for a task: a task that reaches no child ends `NotDispatched`, and its
    /// claim goes back to the queue.
    fn dispatch(
        &self,
        children: &WorkerChildren,
        writer: &Arc<OutcomeWriter>,
        writes: &mut JoinSet<()>,
        claimed: Claimed,
        count: StartCount,
    ) {
        let task = claimed.task.clone();
        let submitted = children.submit(claimed);
        let writer = Arc::clone(writer);
        let released = Arc::clone(&self.capacity_released);
        writes.spawn(async move {
            let end = attempt_end(submitted).await;
            let settled = writer.settle(&task, end).await;
            tracing::debug!(task_id = %task.task_id, settlement = ?settled, "attempt end settled");
            // The claim leaves the start check before the wake, so the pass
            // that the wake runs counts without it.
            drop(count);
            released.notify_one();
        });
    }

    /// The claim loop (Python `run_forever`): one pass per wake; a pass that
    /// may have left work no wake announces runs again at once. A retryable
    /// error backs off and claims again; any other error, or a retryable one
    /// after the backoff, is returned and stops the worker.
    async fn claim_loop(
        &self,
        children: &WorkerChildren,
        writer: &Arc<OutcomeWriter>,
        writes: &mut JoinSet<()>,
        hold: &mut QueueCapHold,
        notifications: &mut mpsc::Receiver<sqlx::postgres::PgNotification>,
    ) -> Option<WorkerError> {
        let steps = BrokerClaimSteps::new(Arc::clone(&self.broker), self.worker_id.clone());
        let mut claim_backoff = RetryBackoff::from_config(&self.app_config.resilience);
        let mut short_passes: u32 = 0;
        let poll_interval =
            Duration::from_millis(self.app_config.resilience.notify_poll_interval_ms);
        loop {
            if self.cancel.is_cancelled() {
                return None;
            }
            reap_finished(writes);
            match self
                .claim_and_dispatch(&steps, children, writer, writes, hold)
                .await
            {
                Ok(counts) => {
                    claim_backoff.reset();
                    let repass =
                        counts.left_unannounced_work(self.claim_batch_limit(), short_passes);
                    short_passes = counts.short_passes_after(short_passes);
                    if repass {
                        continue;
                    }
                }
                Err(error) if error.is_retryable() && claim_backoff.can_retry() => {
                    let delay = claim_backoff.next_delay_seconds();
                    tracing::warn!(
                        error = %error,
                        attempt = claim_backoff.attempts(),
                        delay_s = format!("{:.2}", delay),
                        "retryable claim error, backing off",
                    );
                    tokio::select! {
                        _ = self.cancel.cancelled() => return None,
                        _ = tokio::time::sleep(Duration::from_secs_f64(delay)) => {}
                    }
                    continue;
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        attempts = claim_backoff.attempts(),
                        retryable = error.is_retryable(),
                        "claim error with no retry left; shutting down",
                    );
                    return Some(error);
                }
            }
            match wait_for_claim_wake(
                &self.cancel,
                &self.capacity_released,
                notifications,
                poll_interval,
            )
            .await
            {
                ClaimWake::Cancelled => return None,
                ClaimWake::ListenerClosed => {
                    tracing::info!("listener channel closed, shutting down");
                    return None;
                }
                ClaimWake::DatabaseNotification => {
                    for _ in 0..self.worker_config.coalesce_notifies {
                        match notifications.try_recv() {
                            Ok(_) => {}
                            Err(
                                mpsc::error::TryRecvError::Empty
                                | mpsc::error::TryRecvError::Disconnected,
                            ) => break,
                        }
                    }
                }
                ClaimWake::CapacityReleased | ClaimWake::PollElapsed => {}
            }
        }
    }

    /// Serve: the listener, the service loops and the claim loop on started
    /// children; then the stop.
    async fn serve(
        &self,
        running: RunningChildren,
        batcher: FinalizeBatcher<WorkerPoolSuccessWrite>,
    ) -> Result<(), WorkerError> {
        let RunningChildren {
            children,
            spawner,
            live,
        } = running;

        // Print the startup banner before any log lines.
        crate::worker::cli::banner::print_banner(&crate::worker::cli::banner::BannerInfo {
            worker_id: &self.worker_id,
            worker_config: &self.worker_config,
            app_config: &self.app_config,
            profile: &self.profile.profile,
            tasks: &self.registered,
            role: "worker",
        });
        tracing::info!(
            worker_id = %self.worker_id,
            queues = ?self.worker_config.queues,
            profile = %self.profile.name,
            cluster_wide_cap = ?self.app_config.cluster_wide_cap,
            prefetch_buffer = self.app_config.prefetch_buffer,
            claim_lease_ms = ?self.app_config.claim_lease_ms,
            "worker starting",
        );
        self.log_claim_settings(&children);

        // Subscribe to queue notifications with resilience.
        let listener = match self.connect_listener_with_resilience().await {
            Ok(listener) => listener,
            Err(error) => {
                RunningChildren {
                    children,
                    spawner,
                    live,
                }
                .stop_after_failed_start()
                .await;
                return Err(error);
            }
        };
        let mut notify_rx = self.spawn_listener(listener);

        let writer = Arc::new(OutcomeWriter::new(
            Arc::clone(&self.broker),
            Arc::clone(&self.workflow_registry),
            self.worker_id.clone(),
            self.app_config.payload.clone(),
            self.app_config.retention.clone(),
            batcher.clone(),
            OrphanSelfHeal::of_recovery_flag(
                self.app_config
                    .recovery
                    .auto_terminate_orphaned_workflow_tasks,
            ),
        ));
        let stops = ChildrenStopCell::default();
        let services = match self.spawn_services(&children, &live, &stops) {
            Ok(services) => services,
            Err(error) => {
                self.cancel.cancel();
                RunningChildren {
                    children,
                    spawner,
                    live,
                }
                .stop_after_failed_start()
                .await;
                return Err(error);
            }
        };
        tracing::info!(worker_id = %self.worker_id, "worker ready");

        let mut writes: JoinSet<()> = JoinSet::new();
        // Soft-cap mode: the claimed rows of each capped queue wait here
        // until a claim pass finds room under the queue's cap.
        let mut hold = QueueCapHold::new(StartCheckCaps::of_mode(
            self.app_config.prefetch_buffer,
            &self.worker_config.queue_max_concurrency,
        ));
        let fatal_claim_error = self
            .claim_loop(&children, &writer, &mut writes, &mut hold, &mut notify_rx)
            .await;

        // Stop the claim loop and every service loop on every exit path,
        // including a fatal claim error that broke the loop without a prior
        // cancel (C11). Idempotent when a signal already fired the token.
        self.cancel.cancel();
        let drain = self.profile.profile.drain_timeout.get();
        RunningChildren::stop_pools(children, drain).await;
        // Rows that waited for their queue cap never reached a child: return
        // them to their queue, with the writes that the drain below bounds.
        return_held_rows(&mut hold, &writer, &mut writes);
        // After the pools: every end is known, so the writes have database
        // work left only. A write that the drain cancels leaves its row in
        // the batcher, and the close still writes it.
        let _stopped = drain_then_close(&mut writes, OUTCOME_WRITE_DRAIN, &batcher).await;
        // Last: the runner heartbeats covered the drain above, and their
        // watchdog keeps the exit bound of a fail-closed stop until here.
        let services = services.stop_runner_beats().await;
        RunningChildren::stop_spawner(spawner).await;
        tracing::info!("Worker stopped");
        Self::stop_result(services, stops, fatal_claim_error)
    }

    /// The result of a stopped worker, the first cause that holds: the
    /// runner heartbeats failed closed; the children stopped the worker; a
    /// fatal claim error; a service loop died. A failed start hook stops the
    /// worker gracefully.
    fn stop_result(
        mut services: ServicesStopped,
        stops: ChildrenStopCell,
        fatal_claim_error: Option<WorkerError>,
    ) -> Result<(), WorkerError> {
        match services.fail_closed_stop.state() {
            FailClosedState::Watching => {}
            FailClosedState::FailedClosed { cause } => {
                return Err(WorkerError::RunnerHeartbeatLost { cause });
            }
        }
        match stops.take() {
            None | Some(ChildrenStop::HookFailed) => {}
            Some(ChildrenStop::PoolFailed(failure)) => {
                return Err(WorkerError::PoolFailed(failure))
            }
            Some(ChildrenStop::BaselineOverLimit(over)) => {
                return Err(WorkerError::BaselineOverLimit(over))
            }
        }
        if let Some(error) = fatal_claim_error {
            return Err(error);
        }
        // A service-loop death fired the token to get here; surface it so the
        // process exits non-zero instead of reporting a clean shutdown
        // (C11-inverse).
        if let Ok(exit) = services.fatal_rx.try_recv() {
            return Err(WorkerError::ServiceLoopDied {
                service: exit.service,
                reason: exit.reason,
            });
        }
        Ok(())
    }

    /// Fan out LISTEN/NOTIFY to a bounded channel of claim wakes, and serve
    /// liveness pings off that path. Notifications are coalesceable wake-up
    /// signals, so dropping overflow is safe. Ends at the stop.
    fn spawn_listener(
        &self,
        mut listener: NotifyListener,
    ) -> mpsc::Receiver<sqlx::postgres::PgNotification> {
        let (notify_tx, notify_rx) = mpsc::channel(256);
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
        notify_rx
    }

    /// Start the service loops: the claimer heartbeat, the reaper, the
    /// worker snapshot, the runner heartbeats of the tasks that children
    /// hold, the watch of the pool failure and the new-kind baseline guard.
    /// Each runs under a watchdog: one that ends with no stop request stops
    /// the worker with `ServiceLoopDied` (C11-inverse).
    ///
    /// Errors: [`WorkerError::Config`] when the runner heartbeat watchdog
    /// thread does not start.
    fn spawn_services(
        &self,
        children: &WorkerChildren,
        live: &LiveDispatches,
        stops: &ChildrenStopCell,
    ) -> Result<Services, WorkerError> {
        let claimer_interval =
            Duration::from_millis(self.app_config.recovery.claimer_heartbeat_interval_ms);
        let claimer_hb = spawn_claimer_heartbeat(
            self.broker.pool().clone(),
            self.worker_id.clone(),
            self.hostname.clone(),
            std::process::id() as i32,
            claimer_interval,
            effective_claim_lease_ms(&self.app_config),
            self.app_config.max_claim_renew_age_ms,
            self.cancel.clone(),
        );
        let reaper_health = new_reaper_health();
        let reaper = spawn_reaper(
            self.broker.pool().clone(),
            self.broker.session_pool().clone(),
            Arc::clone(&self.workflow_registry),
            self.app_config.recovery.clone(),
            self.app_config.payload.clone(),
            self.app_config.retention.clone(),
            Arc::clone(&reaper_health),
            self.cancel.clone(),
        );
        let state_loop = crate::worker::worker_state::spawn_worker_state_loop(
            self.broker.pool().clone(),
            self.worker_id.clone(),
            self.hostname.clone(),
            std::process::id() as i32,
            self.worker_config.clone(),
            self.app_config.clone(),
            reaper_health,
            children.view(),
            ClaimColumns::of_profile(&self.profile.profile),
            Utc::now(),
            self.cancel.clone(),
        );
        // Runner heartbeats: one fenced statement per interval for every
        // task a child holds, on its own connection, and a fail-closed
        // watchdog thread (exit 77). They run until the pools drained, so
        // they have their own token, cancelled after the outcome writes. A
        // watchdog that fails closed kills the children that hold tasks and
        // stops the worker through its token.
        let beats_cancel = CancellationToken::new();
        let fail_closed_stop = FailClosedStop::new(self.cancel.clone());
        let (runner_beat_watch, runner_beats) = launch_runner_beats(
            runner_beat_pool(self.broker.pool()),
            BeatSender {
                worker_id: self.worker_id.clone(),
                hostname: self.hostname.clone(),
                pid: std::process::id() as i32,
            },
            Duration::from_millis(self.app_config.recovery.runner_heartbeat_interval_ms),
            StaleRunningRecovery::from_config(&self.app_config.recovery),
            live.clone(),
            beats_cancel.clone(),
            fail_closed_stop.clone(),
        )
        .map_err(|error| {
            WorkerError::Config(format!(
                "the runner heartbeat watchdog thread did not start: {error}"
            ))
        })?;
        let pool_failure = tokio::spawn(watch_pool_failure(
            Arc::clone(children.board()),
            stops.clone(),
            self.cancel.clone(),
        ));
        let new_kind_baselines = tokio::spawn(watch_new_kind_baselines(
            Arc::clone(children.board()),
            baseline_limits(&self.profile.profile),
            stops.clone(),
            self.cancel.clone(),
        ));

        let (fatal_tx, fatal_rx) =
            mpsc::unbounded_channel::<crate::worker::supervision::ServiceExit>();
        let watched: [(&'static str, tokio::task::JoinHandle<()>); 6] = [
            ("claimer-heartbeat", claimer_hb),
            ("runner-heartbeat", runner_beats),
            ("reaper", reaper),
            ("worker-state", state_loop),
            ("slot-failure-watch", pool_failure),
            ("new-kind-baseline-watch", new_kind_baselines),
        ];
        for (service, handle) in watched {
            crate::worker::supervision::spawn_service_watchdog(
                service,
                handle,
                self.cancel.clone(),
                fatal_tx.clone(),
            );
        }
        drop(fatal_tx);
        Ok(Services {
            fatal_rx,
            beats_cancel,
            runner_beat_watch,
            fail_closed_stop,
        })
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
}

/// Join the outcome writes that ended, so the set holds only the writes in
/// flight. A write that panicked is a defect: its panic is raised here.
fn reap_finished(writes: &mut JoinSet<()>) {
    while let Some(joined) = writes.try_join_next() {
        if let Err(error) = joined {
            if let Ok(panic) = error.try_into_panic() {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::execution::{
        execute_and_finalize, notify_worker_capacity, persist_terminal_state, run_phase2,
        FinalizeStage,
    };

    use crate::async_task_fn;
    use crate::broker::{ClaimedTaskRow, NotifyListener, PostgresBroker};
    use crate::core::config::recovery::RecoveryConfig;
    use crate::core::history::maintenance::coverage::{
        ensure_startup_coverage, StartupCoverageOutcome,
    };
    use crate::core::history::reads::publisher::StagedLoaderPublisher;
    use crate::core::registry::WorkflowSpecRegistry;
    use crate::core::task::fn_trait::{AsyncTaskFn, RawTaskResult, RegisteredTask, TaskMeta};
    use crate::core::task::{OperationalErrorCode, TaskError, TaskErrorCode, TaskResult, UserCode};
    use chrono::Utc;
    use futures::FutureExt;
    use serial_test::serial;
    use sqlx::PgPool;
    use std::future::Future;
    use std::panic::{resume_unwind, AssertUnwindSafe};
    use std::pin::Pin;
    use std::sync::Arc;
    use uuid::Uuid;

    /// The end of an outcome write wakes the claim loop before its poll
    /// deadline (Python `_capacity_released`, set by each finalizer).
    #[tokio::test]
    async fn released_capacity_wakes_claim_wait_before_poll_deadline() {
        let released = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let (_sender, mut receiver) = mpsc::channel(1);
        let wait = wait_for_claim_wake(&cancel, &released, &mut receiver, Duration::from_secs(60));
        tokio::pin!(wait);
        assert!(wait.as_mut().now_or_never().is_none());
        released.notify_one();
        let wake = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("local capacity must wake the claim loop before polling");
        assert_eq!(wake, ClaimWake::CapacityReleased);
    }

    /// A write that ends during a claim pass leaves the wake set, so the
    /// next wait returns at once (Python: the loop clears the event before
    /// each pass).
    #[tokio::test]
    async fn released_capacity_is_retained_before_claim_wait_starts() {
        let released = Arc::new(Notify::new());
        released.notify_one();
        let cancel = CancellationToken::new();
        let (_sender, mut receiver) = mpsc::channel(1);
        let wake = tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_claim_wake(&cancel, &released, &mut receiver, Duration::from_secs(60)),
        )
        .await
        .expect("release before waiting must not lose the wake");
        assert_eq!(wake, ClaimWake::CapacityReleased);
        assert!(
            wait_for_claim_wake(&cancel, &released, &mut receiver, Duration::from_secs(60),)
                .now_or_never()
                .is_none(),
            "an idle worker must not keep waking"
        );
    }

    async fn test_pool() -> PgPool {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut transaction = pool.begin().await.expect("coverage transaction");
        let coverage =
            ensure_startup_coverage(transaction.as_mut(), 2, 2, &[], &StagedLoaderPublisher)
                .await
                .expect("startup coverage");
        assert!(matches!(coverage, StartupCoverageOutcome::Ready(_)));
        transaction.commit().await.expect("commit startup coverage");
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
        Arc::new(PostgresBroker::from_pool(test_pool().await))
    }

    /// The claim generation of every row that `insert_claimed_task` seeds and
    /// of every dispatch row that `claimed_task_row` builds, so a test's
    /// dispatch fences on the generation its row carries.
    fn test_claim_generation() -> chrono::DateTime<Utc> {
        static GENERATION: std::sync::OnceLock<chrono::DateTime<Utc>> = std::sync::OnceLock::new();
        *GENERATION.get_or_init(Utc::now)
    }

    async fn insert_claimed_task(
        pool: &PgPool,
        task_id: &Uuid,
        queue_name: &str,
        retry_count: i32,
        max_retries: i32,
        task_options: Option<&str>,
    ) {
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
                    $1, 'finalize_test', $2, 100, 'null', $5, 'ISOLATED',
                    'standard_30d', NOW(), NOW(),
                    1, decode(repeat('00', 32), 'hex'), NULL,
                    'CLAIMED', NOW(), $6, 'worker-1', NOW() + interval '1 minute',
                    NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    $3, $4, NULL, NULL, FALSE,
                    NOW(), FALSE, NULL, NULL
                )",
        )
        .bind(*task_id)
        .bind(queue_name)
        .bind(retry_count)
        .bind(max_retries)
        .bind(task_options)
        .bind(test_claim_generation())
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_running_task(
        pool: &PgPool,
        task_id: &Uuid,
        queue_name: &str,
        retry_count: i32,
        max_retries: i32,
        task_options: Option<&str>,
    ) {
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
                    $1, 'finalize_test', $2, 100, 'null', $5, 'ISOLATED',
                    'standard_30d', NOW(), NOW(),
                    1, decode(repeat('00', 32), 'hex'), NULL,
                    'RUNNING', NOW(), NULL, NULL, NULL,
                    NOW(), NULL, NULL,
                    NULL, NULL, NULL,
                    $3, $4, NULL, NULL, FALSE,
                    NOW(), FALSE, NULL, NULL
                )",
        )
        .bind(*task_id)
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
        task_id: &Uuid,
        queue_name: &str,
        task_options: Option<String>,
    ) -> ClaimedTaskRow {
        ClaimedTaskRow {
            id: *task_id,
            task_name: "finalize_test".to_owned(),
            input: "null".to_owned(),
            queue_name: queue_name.to_owned(),
            is_workflow_task: false,
            task_options,
            claimed_at: test_claim_generation(),
        }
    }

    /// Like `claimed_task_row` but marked as a workflow task, mirroring a row
    /// claimed for a task that `link_task_to_workflow` attached to a workflow.
    fn claimed_workflow_task_row(
        task_id: &Uuid,
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
        task_id: &Uuid,
        output_task_index: Option<i32>,
    ) -> Uuid {
        let workflow_id = Uuid::new_v4();
        let workflow_task_id = Uuid::new_v4();

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
        .bind(workflow_id)
        .bind(output_task_index)
        .execute(pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, task_id,
                created_at, max_retries, execution, retention_class_key
            ) VALUES (
                $1, $2, 0, 'node_0', 'finalize_test', 'null', 'default', 100, '{}', FALSE,
                'all', 'ENQUEUED', FALSE, $3, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(workflow_task_id)
        .bind(workflow_id)
        .bind(*task_id)
        .execute(pool)
        .await
        .unwrap();

        // A task linked into a workflow is a workflow task; mirror production
        // (workflow inserts set is_workflow_task = TRUE) so phase-2 routing,
        // which now reads the carried column, advances the workflow.
        sqlx::query(
            "UPDATE horsies_tasks
             SET is_workflow_task = TRUE,
                 command_fingerprint = NULL,
                 command_fingerprint_version = NULL
             WHERE id = $1",
        )
        .bind(*task_id)
        .execute(pool)
        .await
        .unwrap();

        workflow_id
    }

    async fn fetch_task_state(
        pool: &PgPool,
        task_id: &Uuid,
    ) -> (String, Option<String>, Option<String>) {
        sqlx::query_as(
            "SELECT status, result, error_code
             FROM (
                 SELECT status, result, error_code, 0 AS source
                 FROM horsies_tasks WHERE id = $1
                 UNION ALL
                 SELECT status, convert_from(result_payload, 'UTF8') AS result,
                        error_code, 1 AS source
                 FROM horsies_task_history WHERE task_id = $1
             ) AS task_state
             ORDER BY source
             LIMIT 1",
        )
        .bind(*task_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn fetch_attempt_state(
        broker: &PostgresBroker,
        task_id: &Uuid,
    ) -> (String, bool, Option<String>) {
        let attempts = broker.get_task_attempts(*task_id).await.unwrap();
        let attempt = attempts.last().expect("first attempt");
        (
            attempt.outcome.clone(),
            attempt.will_retry,
            attempt.error_code.clone(),
        )
    }

    async fn fetch_workflow_state(pool: &PgPool, workflow_id: &Uuid) -> (String, Option<String>) {
        sqlx::query_as("SELECT status, result FROM horsies_workflows WHERE id = $1")
            .bind(*workflow_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn fetch_workflow_task_status(
        pool: &PgPool,
        workflow_id: &Uuid,
        task_id: &Uuid,
    ) -> String {
        sqlx::query_scalar(
            "SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_id = $2",
        )
        .bind(*workflow_id)
        .bind(*task_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn fetch_only_workflow_task_state(
        pool: &PgPool,
        workflow_id: &Uuid,
    ) -> (String, Option<Uuid>) {
        sqlx::query_as(
            "SELECT status, task_id
             FROM horsies_workflow_tasks
             WHERE workflow_id = $1
             LIMIT 1",
        )
        .bind(*workflow_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn fetch_attempt_count(pool: &PgPool, task_id: &Uuid) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1")
            .bind(*task_id)
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

    /// Python `horsies/core/cli.py:886` and `:955`: the memory fit of the
    /// profile is logged before the database start, the connection fit after
    /// it and before the banner and the claim loop; neither returns an error.
    #[test]
    fn the_profile_fit_is_logged_at_boot_before_any_claim() {
        let source = include_str!("worker.rs");
        let run = &source[source
            .find("pub async fn run(&self)")
            .expect("Worker::run source")..];
        let memory = run
            .find("log_memory_fit_at_boot(&self.profile)")
            .expect("memory fit at boot");
        let children = run
            .find("self.start_pools().await")
            .expect("children start");
        let start = run
            .find("self.start_database_with_resilience().await")
            .expect("database start with retry");
        let connections = run
            .find(
                "log_connection_fit_at_boot(&self.profile, &self.app_config.broker, &self.broker)",
            )
            .expect("connection fit at boot");
        let banner = run.find("print_banner(").expect("startup banner");
        let claims = run.find("claim_for_dispatch(").expect("claim loop");
        assert!(memory < children);
        // Python `start`: the children start before any database work, so a
        // bad app or hook fails at once.
        assert!(children < start);
        assert!(memory < start);
        assert!(start < connections);
        assert!(connections < banner);
        assert!(connections < claims);
    }

    #[test]
    fn startup_authorization_and_coverage_precede_runtime_side_effects() {
        let source = include_str!("worker.rs");
        let run = &source[source
            .find("pub async fn run(&self)")
            .expect("Worker::run source")
            .to_owned()..];
        let start = run
            .find("self.start_database_with_resilience().await")
            .expect("database start with retry");
        let database = &source[source
            .find("async fn start_database(&self)")
            .expect("database start steps")..];
        let schema = database
            .find("ensure_schema_initialized().await")
            .expect("schema authorization");
        let coverage = database
            .find("ensure_startup_coverage_in_pool(")
            .expect("startup partition coverage");
        let session_pool = database[coverage..]
            .find("self.broker.session_pool()")
            .map(|offset| coverage + offset)
            .expect("startup coverage session pool");
        let banner = run.find("print_banner(").expect("startup banner");
        let listener = run
            .find("connect_listener_with_resilience().await")
            .expect("listener setup");
        let heartbeats = run
            .find("spawn_claimer_heartbeat")
            .expect("heartbeat writer startup");
        let claims = run.find("claim_for_dispatch(").expect("claim loop");
        let reaper = run.find("spawn_reaper(").expect("reaper startup");
        let reaper_call = &run[reaper..reaper + 500];
        assert!(reaper_call.contains("self.broker.pool().clone()"));
        assert!(reaper_call.contains("self.broker.session_pool().clone()"));
        assert!(schema < coverage);
        assert!(coverage < session_pool);
        assert!(start < banner);
        assert!(start < listener);
        assert!(start < heartbeats);
        assert!(start < claims);
    }

    // -- the worker on the executor --

    /// A worker of one isolated child on a broker whose pool reaches no
    /// server: any database work fails, so a test sees whether some ran.
    fn worker_of_one_child(command: crate::worker::executor::supervisor::ChildCommand) -> Worker {
        use crate::worker::executor::protocol::{ChildLogLevel, ChildProgram, DatabaseUrl};
        let url = "postgresql://horsies@127.0.0.1:1/horsies";
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy(url)
            .expect("a lazy pool opens no connection");
        Worker::new(
            Arc::new(PostgresBroker::from_pool(pool)),
            Arc::new(WorkflowSpecRegistry::new()),
            AppConfig::for_database_url(url),
            WorkerConfig::default(),
            SelectedProfile {
                name: "default".to_owned(),
                profile: serde_json::from_value(serde_json::json!({
                    "isolated": {"kind": "isolated_children", "count": 1}
                }))
                .expect("a valid test profile"),
            },
            ChildLaunch {
                command,
                program: ChildProgram::Rust,
                log_level: ChildLogLevel::Info,
                database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
            },
            RegisteredClasses::default(),
        )
        .expect("a valid test worker")
    }

    /// Python `test_shared_dispatch.py::TestCapacityAndConfig::test_a_worker_with_no_child_is_refused`.
    #[test]
    fn a_worker_with_no_child_is_refused() {
        use crate::worker::executor::protocol::{ChildLogLevel, ChildProgram, DatabaseUrl};
        let url = "postgresql://horsies@127.0.0.1:1/horsies";
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy(url)
            .expect("a lazy pool opens no connection");
        let refused = Worker::new(
            Arc::new(PostgresBroker::from_pool(pool)),
            Arc::new(WorkflowSpecRegistry::new()),
            AppConfig::for_database_url(url),
            WorkerConfig::default(),
            SelectedProfile {
                name: "empty".to_owned(),
                profile: crate::core::config::worker_profile::WorkerProfile::default(),
            },
            ChildLaunch {
                command: crate::worker::executor::slots::scripted::idle_child_of_any_kind(),
                program: ChildProgram::Rust,
                log_level: ChildLogLevel::Info,
                database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
            },
            RegisteredClasses::default(),
        );
        match refused {
            Err(WorkerError::Config(message)) => {
                assert!(
                    message.contains("the profile starts no children"),
                    "{message}"
                )
            }
            Err(other) => panic!("another error: {other}"),
            Ok(_) => panic!("a worker with no child must be refused"),
        }
    }

    /// Python `test_worker_helpers.py::TestSlotPoolSetup::test_hook_failure_raises_the_hook_error`:
    /// a start hook that fails in the first child ends the start with the
    /// hook error, which no retry takes.
    #[tokio::test]
    async fn a_failed_start_hook_ends_the_start_with_the_hook_error() {
        let worker = worker_of_one_child(
            crate::worker::executor::slots::scripted::hook_failing_child_of_any_kind(),
        );
        let started = tokio::time::timeout(Duration::from_secs(60), worker.start_pools())
            .await
            .expect("the start ends within the bound");
        match started {
            Err(error @ WorkerError::ChildHookFailed) => {
                assert!(!error.is_retryable());
                assert_eq!(error.to_string(), CHILD_HOOK_FAILURE_MESSAGE);
            }
            Err(other) => panic!("another error: {other}"),
            Ok(_) => panic!("a failed start hook must fail the start"),
        }
    }

    /// Python `test_worker_helpers.py::TestSlotPoolSetup::test_a_stop_during_the_start_is_no_failure`
    /// and `::test_a_stop_during_the_start_opens_no_database_work`: a stop
    /// before the children are ready ends the run with no error, before any
    /// database work (the broker reaches no server, so a database step would
    /// fail).
    #[tokio::test]
    async fn a_stop_during_the_start_is_no_failure_and_opens_no_database_work() {
        let worker =
            worker_of_one_child(crate::worker::executor::slots::scripted::idle_child_of_any_kind());
        worker.request_stop();
        let ended = tokio::time::timeout(Duration::from_secs(60), worker.run())
            .await
            .expect("the run ends within the bound");
        assert!(ended.is_ok(), "{ended:?}");
    }

    /// Python `test_worker_helpers.py::TestSlotFailureLoop`: a failed start
    /// hook in a replacement child stops the worker gracefully; a pool that
    /// cannot start a child stops it with an error.
    #[tokio::test]
    async fn a_pool_failure_stops_the_worker_and_names_how() {
        use crate::worker::executor::slots::ChildLayout;
        use crate::worker::executor::supervisor::{BootFailure, BootFailureCause};

        let failures = [
            (PoolFailure::ChildHookFailed, ChildrenStop::HookFailed),
            (
                PoolFailure::BootAttemptsExhausted {
                    attempts: 3,
                    last: BootFailure::new(BootFailureCause::SpawnFailed, "no such program"),
                },
                ChildrenStop::PoolFailed(PoolFailure::BootAttemptsExhausted {
                    attempts: 3,
                    last: BootFailure::new(BootFailureCause::SpawnFailed, "no such program"),
                }),
            ),
        ];
        for (failure, expected) in failures {
            let board = Arc::new(PoolBoard::new(ChildLayout {
                isolated: 1,
                shared_async: 0,
                shared_thread: 0,
            }));
            let stops = ChildrenStopCell::default();
            let cancel = CancellationToken::new();
            let watch = tokio::spawn(watch_pool_failure(
                Arc::clone(&board),
                stops.clone(),
                cancel.clone(),
            ));
            board.fail(failure);
            tokio::time::timeout(Duration::from_secs(5), watch)
                .await
                .expect("the watch ends at the failure")
                .expect("the watch joins");
            assert!(cancel.is_cancelled());
            assert_eq!(stops.take(), Some(expected));
        }
    }

    fn stopped_services() -> ServicesStopped {
        ServicesStopped {
            fatal_rx: mpsc::unbounded_channel().1,
            fail_closed_stop: FailClosedStop::new(CancellationToken::new()),
        }
    }

    /// The result of a stopped worker: a failed start hook is a graceful
    /// stop; a pool failure, a baseline over its limit and a fatal claim
    /// error each end the worker with their error.
    #[test]
    fn the_stop_result_names_the_cause_of_the_stop() {
        use crate::worker::executor::slots::ExecutionClass;

        let ended = |stop: Option<ChildrenStop>, claim: Option<WorkerError>| {
            let stops = ChildrenStopCell::default();
            if let Some(stop) = stop {
                stops.stop_worker(stop, &CancellationToken::new());
            }
            Worker::stop_result(stopped_services(), stops, claim)
        };
        assert!(ended(None, None).is_ok());
        assert!(ended(Some(ChildrenStop::HookFailed), None).is_ok());
        assert!(matches!(
            ended(
                Some(ChildrenStop::PoolFailed(PoolFailure::ChildHookFailed)),
                None
            ),
            Err(WorkerError::PoolFailed(_))
        ));
        let over = BaselineOverLimit {
            class: ExecutionClass::SharedAsync,
            baseline_mb: 600.0,
            limit_mb: std::num::NonZeroU64::new(512).expect("512"),
        };
        assert!(matches!(
            ended(Some(ChildrenStop::BaselineOverLimit(over)), None),
            Err(WorkerError::BaselineOverLimit(_))
        ));
        assert!(matches!(
            ended(None, Some(WorkerError::Config("refused".to_owned()))),
            Err(WorkerError::Config(_))
        ));
    }

    /// Python `test_worker_helpers.py::TestDispatchOne::test_finalizer_gets_the_handle_and_the_claim_fields`:
    /// the task of a dispatch carries every claim field the outcome write
    /// fences on, the class, the retry facts and the stored options read
    /// once; the input travels apart.
    #[test]
    fn a_claimed_row_becomes_the_task_of_its_dispatch() {
        use crate::core::task::Execution;
        use crate::worker::retry::read_stored_options;

        let claimed_at = Utc::now();
        let row = ClaimedRow {
            id: Uuid::from_u128(7),
            task_name: "app.tasks.work".to_owned(),
            queue_name: "q".to_owned(),
            input: r#"{"n": 1}"#.to_owned(),
            task_options: Some(r#"{"timeout_ms": 2000}"#.to_owned()),
            execution: Execution::SharedThread,
            is_workflow_task: true,
            claimed_at,
            retry_count: 1,
            max_retries: 3,
        };

        let claimed = claimed_of(row);

        assert_eq!(
            claimed.task,
            ClaimedTask {
                task_id: Uuid::from_u128(7),
                task_name: "app.tasks.work".to_owned(),
                queue_name: "q".to_owned(),
                claimed_at,
                execution: Execution::SharedThread,
                kind: TaskKind::WorkflowTask,
                retry: RetryFacts {
                    retry_count: 1,
                    max_retries: 3,
                },
                options: read_stored_options(Some(r#"{"timeout_ms": 2000}"#)),
            }
        );
        assert_eq!(claimed.input.as_str(), r#"{"n": 1}"#);
    }

    /// Python `Worker.stop` and `test_runner_heartbeat.py::test_stop_ends_the_heartbeat_after_the_children`:
    /// the claim loop ends, then the pools stop with the drain timeout, then
    /// the rows held for their queue cap go back to the queue (Python
    /// `_return_held_rows`), then the outcome writes drain and the batcher
    /// closes, then the runner heartbeats stop, then the spawner thread.
    #[test]
    fn the_stop_runs_in_python_order() {
        let source = include_str!("worker.rs");
        let serve = &source[source
            .find("async fn serve(")
            .expect("Worker::serve source")..];
        let at = |text: &str| {
            serve
                .find(text)
                .unwrap_or_else(|| panic!("{text} in serve"))
        };
        let claim_loop = at(".claim_loop(&children");
        let cancel = at("self.cancel.cancel();\n        let drain");
        let pools = at("RunningChildren::stop_pools(children, drain).await");
        let held = at("return_held_rows(&mut hold, &writer, &mut writes);");
        let writes = at("drain_then_close(&mut writes, OUTCOME_WRITE_DRAIN, &batcher)");
        let beats = at("services.stop_runner_beats().await");
        let spawner = at("RunningChildren::stop_spawner(spawner).await");
        assert!(claim_loop < cancel);
        assert!(cancel < pools);
        assert!(pools < held);
        assert!(held < writes);
        assert!(writes < beats);
        assert!(beats < spawner);
    }

    /// A start attempt that does not finish in time is retryable, as in
    /// Python, where `asyncio.TimeoutError` takes the retry path.
    #[test]
    fn a_timed_out_start_attempt_is_retryable() {
        let error = WorkerError::StartTimedOut {
            bound: START_ATTEMPT_TIMEOUT,
        };
        assert!(error.is_retryable());
        assert_eq!(START_ATTEMPT_TIMEOUT, Duration::from_secs(30));
        assert!(!WorkerError::Config("coverage refused".to_owned()).is_retryable());
    }

    /// Python `test_worker_helpers.py::test_hard_cap_returns_default_lease`:
    /// with no `claim_lease_ms`, claims carry the 60 s default lease.
    #[test]
    fn hard_cap_returns_default_lease() {
        let mut app_config = AppConfig::for_database_url("postgresql://unused/test");
        assert_eq!(app_config.claim_lease_ms, None);
        assert_eq!(
            effective_claim_lease_ms(&app_config),
            crate::core::defaults::DEFAULT_CLAIM_LEASE_MS,
        );
        assert_eq!(effective_claim_lease_ms(&app_config), 60_000);

        app_config.claim_lease_ms = Some(90_000);
        assert_eq!(effective_claim_lease_ms(&app_config), 90_000);
    }

    /// Every claim and every claimer beat use the effective lease: the claim
    /// pass never sends `None`, and the heartbeat always renews.
    #[test]
    fn claims_and_renewals_always_carry_a_lease() {
        let source = include_str!("worker.rs");
        // Production code only: the test module quotes the searched text.
        let production = &source[..source
            .find(&["#[cfg(test)]", "\nmod tests {"].concat())
            .expect("test module marker")];
        let run = &production[production
            .find("pub async fn run(&self)")
            .expect("Worker::run source")..];
        let heartbeat = &run[run
            .find("spawn_claimer_heartbeat(")
            .expect("claimer heartbeat startup")..];
        let heartbeat_call = &heartbeat[..heartbeat.find(");").expect("end of the call")];
        assert!(heartbeat_call.contains("effective_claim_lease_ms(&self.app_config),"));
        assert!(production.contains("claim_lease_ms: claim_lease(&self.app_config),"));
        assert!(!production.contains("claim_lease_ms: self.app_config.claim_lease_ms"));
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

    use std::sync::atomic::{AtomicU32, Ordering};

    static EXECUTION_COUNT: AtomicU32 = AtomicU32::new(0);

    async fn succeed(_: ()) -> Result<String, TaskError> {
        Ok("done".to_owned())
    }

    /// A task that stays RUNNING long enough for a test to observe concurrency.
    async fn block_a_while(_: ()) -> Result<String, TaskError> {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok("done".to_owned())
    }

    async fn retryable_failure(_: ()) -> Result<String, TaskError> {
        Err(TaskError::new("RETRY_ME", "retry me").unwrap())
    }

    async fn fatal_failure(_: ()) -> Result<String, TaskError> {
        Err(TaskError::new("FATAL", "boom").unwrap())
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

        let task_id = Uuid::new_v4();
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

        let (outcome, will_retry, attempt_error_code) =
            fetch_attempt_state(&broker, &task_id).await;
        assert_eq!(outcome, "COMPLETED");
        assert!(!will_retry);
        assert_eq!(attempt_error_code, None);
    }

    /// Insert a RUNNING task owned by a specific worker, simulating a task that
    /// was reaper-requeued and re-claimed (then set RUNNING) by another worker.
    async fn insert_running_task_owned_by(pool: &PgPool, task_id: &Uuid, owner: &str) {
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
                    $1, 'finalize_test', 'default', 100, 'null', NULL, 'ISOLATED',
                    'standard_30d', NOW(), NOW(),
                    1, decode(repeat('00', 32), 'hex'), NULL,
                    'RUNNING', NOW(), NULL, $2, NULL,
                    NOW(), NULL, NULL,
                    4242, 'finalize-host', 'finalize-process',
                    0, 3, NULL, NULL, FALSE,
                    NOW(), FALSE, NULL, NULL
                )",
        )
        .bind(*task_id)
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
            |task_id: Uuid, worker: &str| TerminalizationCommand::CompleteLockedTask {
                task_id,
                fence: PriorLockedRead {
                    worker_id: worker.to_owned(),
                },
                result_json: "{}".to_owned(),
            };
        let fail_as = |task_id: Uuid, worker: &str, reason: Option<&str>| {
            TerminalizationCommand::FailLockedTask {
                task_id,
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
        let t_complete = Uuid::new_v4();
        insert_running_task_owned_by(&pool, &t_complete, "worker-2").await;
        assert!(
            !applied(
                &terminalize(&pool, &complete_as(t_complete, "worker-1"))
                    .await
                    .unwrap()
            ),
            "stale owner must not complete the re-claimed task"
        );
        assert_eq!(fetch_task_state(&pool, &t_complete).await.0, "RUNNING");
        assert!(
            applied(
                &terminalize(&pool, &complete_as(t_complete, "worker-2"))
                    .await
                    .unwrap()
            ),
            "current owner must complete its own task"
        );
        assert_eq!(fetch_task_state(&pool, &t_complete).await.0, "COMPLETED");

        // fail: stale owner rejected, current owner applies.
        let t_fail = Uuid::new_v4();
        insert_running_task_owned_by(&pool, &t_fail, "worker-2").await;
        assert!(
            !applied(
                &terminalize(&pool, &fail_as(t_fail, "worker-1", None))
                    .await
                    .unwrap()
            ),
            "stale owner must not fail the re-claimed task"
        );
        assert_eq!(fetch_task_state(&pool, &t_fail).await.0, "RUNNING");
        assert!(applied(
            &terminalize(&pool, &fail_as(t_fail, "worker-2", None))
                .await
                .unwrap()
        ));
        assert_eq!(fetch_task_state(&pool, &t_fail).await.0, "FAILED");

        // requeue: stale owner rejected, current owner applies.
        let t_requeue = Uuid::new_v4();
        insert_running_task_owned_by(&pool, &t_requeue, "worker-2").await;
        assert!(
            !broker
                .requeue(t_requeue, Some(Utc::now()), "worker-1")
                .await
                .unwrap(),
            "stale owner must not requeue the re-claimed task"
        );
        assert_eq!(fetch_task_state(&pool, &t_requeue).await.0, "RUNNING");
        assert!(broker
            .requeue(t_requeue, Some(Utc::now()), "worker-2")
            .await
            .unwrap());
        assert_eq!(fetch_task_state(&pool, &t_requeue).await.0, "PENDING");

        // worker-crash failure (failed_reason carried): stale owner rejected,
        // current owner applies. Same operation as task failure — they differ
        // only in whether a failure reason travels.
        let t_worker = Uuid::new_v4();
        insert_running_task_owned_by(&pool, &t_worker, "worker-2").await;
        assert!(
            !applied(
                &terminalize(&pool, &fail_as(t_worker, "worker-1", Some("crash")))
                    .await
                    .unwrap()
            ),
            "stale owner must not worker-fail the re-claimed task"
        );
        assert_eq!(fetch_task_state(&pool, &t_worker).await.0, "RUNNING");
        assert!(applied(
            &terminalize(&pool, &fail_as(t_worker, "worker-2", Some("crash")))
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

        let task_id = Uuid::new_v4();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        let workflow_id = link_task_to_workflow(&pool, &task_id, Some(0)).await;

        // Drive the single node terminal so the non-terminal count is zero.
        sqlx::query("UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = '{}', completed_at = NOW() WHERE workflow_id = $1")
            .bind(&workflow_id)
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
            workflow_id,
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

        let task_id = Uuid::new_v4();
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

        let task_id = Uuid::new_v4();
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

        let task_id = Uuid::new_v4();
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

        let task_id = Uuid::new_v4();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        link_task_to_workflow(&pool, &task_id, Some(0)).await;
        EXECUTION_COUNT.store(0, Ordering::SeqCst);

        let (function_name, trigger_name) = install_fail_workflow_task_running_trigger(&pool).await;
        let result = AssertUnwindSafe(async {
            run_finalize(
                &broker,
                async_task_fn!(counted_success_task, ()),
                claimed_workflow_task_row(&task_id, "default", None),
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

            let attempts = broker.get_task_attempts(task_id).await.unwrap();
            assert_eq!(attempts.len(), 1);
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

        let task_id = Uuid::new_v4();
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

        let task_id = Uuid::new_v4();
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

    /// Python `test_stored_timeout_refusal.py::test_unreadable_options_fail_in_one_claim_and_do_not_retry`:
    /// stored options that do not parse fail the task before user code runs,
    /// with WORKER_SERIALIZATION_ERROR, and the task does not retry (its
    /// retry policy is inside the unreadable text).
    #[tokio::test]
    #[serial]
    async fn unreadable_stored_options_fail_the_task_before_user_code_runs() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4();
        insert_claimed_task(&pool, &task_id, "default", 0, 3, Some("{")).await;

        run_finalize(
            &broker,
            async_task_fn!(succeed, ()),
            claimed_task_row(&task_id, "default", Some("{".to_owned())),
        )
        .await;

        let (status, _result, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "FAILED", "a run of `succeed` would have completed");
        assert_eq!(error_code.as_deref(), Some("WORKER_SERIALIZATION_ERROR"));
        let (outcome, will_retry, attempt_error_code) =
            fetch_attempt_state(&broker, &task_id).await;
        assert_eq!(outcome, "FAILED");
        assert!(!will_retry);
        assert_eq!(
            attempt_error_code.as_deref(),
            Some("WORKER_SERIALIZATION_ERROR")
        );
    }

    /// Python `test_stored_timeout_refusal.py::test_a_refused_timeout_retries_until_max_retries_when_asked`:
    /// a stored timeout above the limit refuses the run, and a readable
    /// policy that names WORKER_SERIALIZATION_ERROR still retries it.
    #[tokio::test]
    #[serial]
    async fn a_refused_timeout_retries_when_the_policy_names_the_refusal() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4();
        let task_options = serde_json::json!({
            "timeout_ms": 4_294_967_296_u64,
            "retry_policy": {
                "max_retries": 2,
                "intervals": [1, 1],
                "backoff_strategy": "fixed",
                "jitter": false,
                "max_delay_seconds": null,
                "auto_retry_for": ["WORKER_SERIALIZATION_ERROR"]
            }
        })
        .to_string();
        insert_claimed_task(&pool, &task_id, "default", 0, 2, Some(&task_options)).await;

        run_finalize(
            &broker,
            async_task_fn!(succeed, ()),
            claimed_task_row(&task_id, "default", Some(task_options)),
        )
        .await;

        let (status, _result, _error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "PENDING", "the refusal is retried, not run");
        let (outcome, will_retry, attempt_error_code) =
            fetch_attempt_state(&broker, &task_id).await;
        assert_eq!(outcome, "FAILED");
        assert!(will_retry);
        assert_eq!(
            attempt_error_code.as_deref(),
            Some("WORKER_SERIALIZATION_ERROR")
        );
    }

    #[tokio::test]
    #[serial]
    async fn finalize_retryable_failure_requeues_and_skips_callback() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4();
        // Python's stored layout: auto_retry_for lives inside retry_policy.
        let task_options = serde_json::json!({
            "retry_policy": {
                "max_retries": 3,
                "intervals": [1, 1, 1],
                "backoff_strategy": "fixed",
                "jitter": false,
                "max_delay_seconds": null,
                "auto_retry_for": ["RETRY_ME"]
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

        let (outcome, will_retry, attempt_error_code) =
            fetch_attempt_state(&broker, &task_id).await;
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

        let task_id = Uuid::new_v4();
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
            TaskErrorCode::User(UserCode::parse("FATAL").unwrap())
        );

        let (outcome, will_retry, attempt_error_code) =
            fetch_attempt_state(&broker, &task_id).await;
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
        let task_error_code = OperationalErrorCode::UnhandledError.to_string();

        let task_id = Uuid::new_v4();
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

    /// A database error during the failed-attempt write decides neither the
    /// retry nor the failure: phase 1 reports a phase-1 error and writes
    /// nothing, and the next try sends the same statement (Python
    /// `core/worker/finalize.py`, `_persist_failed_attempt`). Before, an error
    /// of the retry write fell through to the terminal failure write.
    #[tokio::test]
    #[serial]
    async fn a_database_error_in_the_failed_attempt_write_decides_nothing() {
        let pool = test_pool().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4();
        let task_options = serde_json::json!({
            "retry_policy": {
                "max_retries": 3,
                "intervals": [1, 1, 1],
                "backoff_strategy": "fixed",
                "jitter": false,
                "max_delay_seconds": null,
                "auto_retry_for": ["RETRY_ME"]
            }
        })
        .to_string();
        insert_running_task(&pool, &task_id, "default", 0, 3, Some(&task_options)).await;

        let closed_broker = test_broker().await;
        closed_broker.pool().close().await;

        let Err(err) = persist_terminal_state(
            &closed_broker,
            task_id,
            TaskResult::Err(TaskError::new("RETRY_ME", "try again").unwrap()),
            &claimed_task_row(&task_id, "default", Some(task_options.clone())),
            &crate::worker::retry::read_stored_options(Some(&task_options)),
            &crate::broker::SetRunningRow {
                id: task_id,
                started_at: Utc::now(),
                retry_count: 0,
                max_retries: 3,
                good_until: None,
            },
            "worker-1",
            &crate::core::config::payload::PayloadPolicy::default(),
        )
        .await
        else {
            panic!("a closed pool must fail the failed-attempt write");
        };

        assert_eq!(err.stage, FinalizeStage::Phase1Persist);
        let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "RUNNING");
        assert_eq!(result, None);
        assert_eq!(error_code, None);
        assert_eq!(fetch_attempt_count(&pool, &task_id).await, 0);
    }

    #[tokio::test]
    #[serial]
    async fn finalize_phase1_db_failure_leaves_task_running_for_reaper() {
        let pool = test_pool().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4();
        insert_running_task(&pool, &task_id, "default", 0, 0, None).await;

        let closed_broker = test_broker().await;
        closed_broker.pool().close().await;

        let Err(err) = persist_terminal_state(
            &closed_broker,
            task_id,
            TaskResult::Ok(br#""phase1-durable""#.to_vec()),
            &claimed_task_row(&task_id, "default", None),
            &crate::worker::retry::read_stored_options(None),
            &crate::broker::SetRunningRow {
                id: task_id,
                started_at: Utc::now(),
                retry_count: 0,
                max_retries: 0,
                good_until: None,
            },
            "worker-1",
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

        let task_id = Uuid::new_v4();
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

        let (outcome, will_retry, attempt_error_code) =
            fetch_attempt_state(&broker, &task_id).await;
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

        let task_id = Uuid::new_v4();
        notify_worker_capacity(&pool, "high-priority", task_id).await;

        let first = tokio::time::timeout(Duration::from_secs(2), listener.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (first.channel().to_owned(), first.payload().to_owned()),
            (
                "task_queue_high-priority".to_owned(),
                format!("capacity:{task_id}"),
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
        notify_worker_capacity(&pool, "default", Uuid::new_v4()).await;
    }
}

#[cfg(test)]
mod live_worker_tests;

//! The worker: claims rows, runs each task in a child process of the
//! executor, and writes how each attempt ended.
//!
//! User code never runs in the worker process. The worker starts the
//! children of its profile before any database work, then claims with the
//! budgets of the children in service, submits each row to the pool of its
//! kind, and settles each end with the outcome writer. In soft-cap mode a
//! row of a capped queue waits in the parent until its queue's count leaves
//! room (`queue_cap_hold`).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;
use std::time::Duration;

use chrono::Utc;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, Notify};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::broker::claims::{ClaimClasses, ClaimPassRequest, ClaimedRow};
use crate::broker::health::WORKER_PING_CHANNEL;
use crate::broker::process_listener::{default_payload_capacity, fixed_channel};
use crate::broker::PostgresBroker;
use crate::core::config::app::AppConfig;
use crate::core::config::worker_profile::{SelectedProfile, TaskLimitSetting};
use crate::core::history::maintenance::coverage::{
    startup_verdict, CoverageRequest, CoverageSerialization, StartupCoverageOutcome,
};
use crate::db::{
    ChannelName, CheckoutClass, ParameterType, PayloadEvent, PayloadFilter, PayloadSubscription,
    Reserve, StatementBound, TypedStatement, WakeEvent, WakeSubscription,
};
use crate::telemetry::worker::WorkerTelemetry;
use crate::worker::backoff::RetryBackoff;
use crate::worker::children::{
    attempt_end, baseline_limits, exit_not_seen, pending_task_limit, plan_placement, preload_plan,
    ChildLaunch, ChildrenStopped, PoolFacts, WorkerChildren,
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
    AttemptEnd, BaselineOverLimit, ChildrenStart, ChildrenStartFailed, ClaimRoute, Claimed,
    ClaimedTask, NotRunCause, OrphanSelfHeal, OutcomeWriter, PoolBoard, PoolFailure,
    PoolStopReport, RetryFacts, TaskKind,
};
use crate::worker::executor::supervisor::{
    PreloadMode, PreloadStart, PreloadUnsupported, Spawner, SpawnerStopReport, CHILD_KILL_WAIT,
};
use crate::worker::finalize_batch::statement::WorkerPoolSuccessWrite;
use crate::worker::finalize_batch::stop::drain_then_close;
use crate::worker::finalize_batch::{FinalizeBatcher, FlusherBound};
use crate::worker::heartbeat::spawn_claimer_heartbeat;
use crate::worker::liveness::{
    liveness_connect_plan, start_liveness_lock, LivenessRule, LivenessStartError,
    StartedLivenessLock,
};
use crate::worker::profile_fit::{log_connection_fit_at_boot, log_memory_fit_at_boot};
use crate::worker::queue_cap_hold::{admit_held_rows, QueueCapHold, StartCheckCaps, StartCount};
use crate::worker::recovery::{new_reaper_health, spawn_reaper, ReaperHealth};
use crate::worker::retry::read_stored_options;
use crate::worker::run_end::{run_end_of, RunPhase, WorkerRunEnd};
use crate::worker::runner_beats::{
    launch_runner_beats, runner_beat_pool, BeatSender, FailClosedState, FailClosedStop,
    LiveDispatches, RunnerBeatWatch, StaleRunningRecovery,
};
use crate::worker::worker_state::ClaimColumns;
use crate::workflow_engine::child_spec::ChildSpecSource;

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
/// mode alike, so a crashed worker's claims become reclaimable.
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

/// How one attempt of the database start steps ([`Worker::start_database`])
/// ended without an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DatabaseStart {
    /// A heartbeat leaf covers the present instant: the worker goes on.
    Ready,
    /// The startup coverage found the coverage gate held by another process,
    /// and no heartbeat leaf for the present instant
    /// ([`StartupCoverageOutcome::GateBusy`]). The start tries again after a
    /// backoff.
    CoverageGateBusy,
}

/// Why an attempt of the database start did not start the worker, when the
/// start can try again.
#[derive(Debug)]
enum StartRetryCause {
    /// [`DatabaseStart::CoverageGateBusy`].
    CoverageGateBusy,
    /// A retryable error, or an attempt over [`START_ATTEMPT_TIMEOUT`]
    /// ([`WorkerError::StartTimedOut`]). Boxed: the error is large, and this
    /// value lives only for one retry.
    Failed(Box<WorkerError>),
}

impl StartRetryCause {
    /// The error of the start when no retry is left. `waited` is the time
    /// since the first attempt, and `attempts` counts the attempts.
    fn into_final_error(self, waited: Duration, attempts: u64) -> WorkerError {
        match self {
            Self::CoverageGateBusy => WorkerError::CoverageGateBusy { waited, attempts },
            Self::Failed(error) => *error,
        }
    }

    /// The one log line of a retry: an info line for the busy coverage gate,
    /// which another process holds while it creates the leaves; an error line
    /// for an error. `attempt` counts the retries so far.
    fn log_retry(&self, wait: Duration, attempt: u64) {
        match self {
            Self::CoverageGateBusy => tracing::info!(
                attempt,
                retry_in_ms = wait.as_millis(),
                "Worker start waits for the partition coverage gate: another process creates \
                 the history partitions (coverage_gate_busy). Trying again in {} ms",
                wait.as_millis(),
            ),
            Self::Failed(error) => tracing::error!(
                %error,
                attempt,
                delay_s = format!("{:.1}", wait.as_secs_f64()),
                "Worker start failed: {error}. Retrying in {:.1}s (attempt {attempt})",
                wait.as_secs_f64(),
            ),
        }
    }
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

/// The pong of a ping: `$1` the reply channel of the request, `$2` the pong
/// JSON.
static NOTIFY_WORKER_PONG: TypedStatement = TypedStatement::new(
    "notify_worker_pong",
    "SELECT pg_notify($1, $2)",
    &[ParameterType::Text, ParameterType::Text],
);

/// Answer one ping addressed to this worker (or to every worker) with one
/// `pg_notify` on the main core pool of `broker`, in autocommit. A payload
/// that does not parse, or a ping for another worker, is answered with
/// nothing.
///
/// Errors: `BrokerError::Serialization` for a pong that does not encode;
/// `BrokerError::PoolSettings` when a closed handle cannot open its pools
/// again; `BrokerError::Statement` for a failed `pg_notify`. The sender of
/// an unanswered ping times out.
async fn handle_worker_ping(
    broker: &PostgresBroker,
    worker_id: &str,
    hostname: &str,
    pid: i32,
    raw_payload: &str,
) -> Result<(), crate::broker::BrokerError> {
    let request: crate::broker::health::WorkerPingRequest = match serde_json::from_str(raw_payload)
    {
        Ok(request) => request,
        Err(e) => {
            tracing::warn!(error = %e, "Discarding invalid ping payload: {e}");
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

    let (open, _write) = broker.core_pools_for_write()?;
    let statement = &NOTIFY_WORKER_PONG;
    let bound = statement
        .bind()
        .value(&request.reply_channel)
        .value(&payload)
        .finish();
    broker.statement_log().log(statement);
    let _notified = open
        .main()
        .execute(
            CheckoutClass::Batch,
            &bound,
            StatementBound::ConnectionTimeouts,
        )
        .await?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum ClaimWake {
    Cancelled,
    CapacityReleased,
    /// A child became ready to take a task after a boot: a recycled child,
    /// a replacement after a loss, or a slot back from down (#1148).
    ChildReady,
    DatabaseNotification,
    /// The listener reconnected: a notification may have been lost, so the
    /// loop claims at once.
    ListenerGap,
    ListenerClosed,
    PollElapsed,
}

impl ClaimWake {
    fn from_listener(event: WakeEvent) -> ClaimWake {
        match event {
            WakeEvent::Notified => ClaimWake::DatabaseNotification,
            WakeEvent::Gap => ClaimWake::ListenerGap,
            WakeEvent::Stopped => ClaimWake::ListenerClosed,
        }
    }
}

/// One pending wait of [`ClaimWakes::next`].
type QueueWait<'a> = Pin<Box<dyn Future<Output = WakeEvent> + Send + 'a>>;

/// The claim wakes of a worker: one wake subscription per queue channel
/// `task_queue_<queue>` on the broker's listener. A wake subscription holds
/// at most one pending signal, so a burst of inserts on one queue wakes the
/// claim loop once.
struct ClaimWakes {
    subscriptions: Vec<WakeSubscription>,
}

impl ClaimWakes {
    /// The next event of any queue. Cancel-safe: a subscription's `recv`
    /// keeps its signal when it is dropped before it returns. With no
    /// subscription it never returns.
    async fn next(&self) -> WakeEvent {
        let mut waits: Vec<QueueWait<'_>> = self
            .subscriptions
            .iter()
            .map(|subscription| Box::pin(subscription.recv()) as QueueWait<'_>)
            .collect();
        std::future::poll_fn(|context| {
            for wait in waits.iter_mut() {
                if let Poll::Ready(event) = wait.as_mut().poll(context) {
                    return Poll::Ready(event);
                }
            }
            Poll::Pending
        })
        .await
    }

    /// Takes at most `limit` signals that are already pending on the queues,
    /// so one claim pass answers a burst on several queues (Python's
    /// `coalesce_notifies`). Waits for nothing. A stop is not taken: a stopped
    /// subscription reads `Stopped` again at the next wait.
    async fn drain_pending(&self, limit: u32) {
        let mut drained: u32 = 0;
        for subscription in &self.subscriptions {
            if drained >= limit {
                return;
            }
            match tokio::time::timeout(Duration::ZERO, subscription.recv()).await {
                Ok(WakeEvent::Notified | WakeEvent::Gap) => drained += 1,
                Ok(WakeEvent::Stopped) | Err(_) => {}
            }
        }
    }
}

/// Runs `settle`, the write of the end of one dispatched run, then takes the
/// run out of the start check (`count`) and wakes the claim loop
/// (`capacity_released`, Python `_capacity_released`, which every finalizer
/// sets). Every end of a run, a success, a failure or a run that reached no
/// child, ends with this wake, so a free slot or seat is claimed again before
/// the poll interval. The run leaves the start check before the wake, so the
/// pass that the wake runs counts without it.
async fn settle_then_release(
    settle: impl Future<Output = ()>,
    count: StartCount,
    capacity_released: Arc<Notify>,
) {
    settle.await;
    drop(count);
    capacity_released.notify_one();
}

/// The next wake of the claim loop: a stop, the end of an outcome write
/// (`capacity_released`), a child ready after a boot (`board`), a queue
/// notification, or the poll interval.
async fn wait_for_claim_wake(
    cancel: &CancellationToken,
    capacity_released: &Notify,
    board: &PoolBoard,
    claim_wakes: &ClaimWakes,
    poll_interval: Duration,
) -> ClaimWake {
    tokio::select! {
        _ = cancel.cancelled() => ClaimWake::Cancelled,
        _ = capacity_released.notified() => ClaimWake::CapacityReleased,
        () = board.child_ready() => ClaimWake::ChildReady,
        event = claim_wakes.next() => ClaimWake::from_listener(event),
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

/// What the start of the preload process means for the worker (Python
/// `SlotPool._start_preload`). Only the `app` mode refuses: the user chose
/// it, so its failure is reported. In the `library` mode a failure costs
/// child start time, so children use `exec` and the worker goes on.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PreloadOutcome {
    /// Children fork from the preload process.
    Forks,
    /// Children use `exec`; `warning` is logged when there is one.
    Exec { warning: Option<String> },
    /// The worker fails before any child starts.
    Refused { reason: String },
}

/// The outcome of a preload start in `mode`. Pure.
fn preload_outcome(mode: PreloadMode, start: PreloadStart) -> PreloadOutcome {
    match (mode, start) {
        (_, PreloadStart::Ready { .. }) => PreloadOutcome::Forks,
        (
            PreloadMode::App,
            PreloadStart::Unsupported(PreloadUnsupported::NotLinux { platform }),
        ) => PreloadOutcome::Exec {
            warning: Some(format!(
                "App preload needs Linux, this is {platform}. Using exec."
            )),
        },
        (PreloadMode::Library, PreloadStart::Unsupported(PreloadUnsupported::NotLinux { .. })) => {
            PreloadOutcome::Exec { warning: None }
        }
        (_, PreloadStart::Unsupported(PreloadUnsupported::MissingKernelSupport { reason })) => {
            PreloadOutcome::Exec {
                warning: Some(format!(
                    "Child preload cannot run on this host: {reason}. Using exec."
                )),
            }
        }
        (PreloadMode::App, PreloadStart::Refused { reason })
        | (PreloadMode::App, PreloadStart::StartFailed { detail: reason }) => {
            PreloadOutcome::Refused { reason }
        }
        (PreloadMode::Library, PreloadStart::Refused { reason }) => PreloadOutcome::Exec {
            warning: Some(format!(
                "No preload parent, using exec: the preload process refused: {reason}"
            )),
        },
        (PreloadMode::Library, PreloadStart::StartFailed { detail }) => PreloadOutcome::Exec {
            warning: Some(format!("No preload parent, using exec: {detail}")),
        },
        (_, PreloadStart::SpawnerStopped) => PreloadOutcome::Exec {
            warning: Some(
                "No preload parent, using exec: the spawner stopped during the preload start"
                    .to_owned(),
            ),
        },
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
    /// spawner thread. Logs the tasks whose children the drain deadline
    /// killed (Python `Worker.stop`).
    async fn stop_pools(children: WorkerChildren, drain: Duration) {
        let ChildrenStopped { report } = children.stop(drain).await;
        let killed = match report {
            PoolStopReport::Drained => Vec::new(),
            PoolStopReport::KilledAtDrainDeadline { killed }
            | PoolStopReport::AbortedAfterDrain { killed } => killed,
        };
        if let Some(line) = drain_kill_line(drain, &killed) {
            tracing::error!("{line}");
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
            Err(error) => tracing::error!(
                error = %error,
                "stopping the spawner thread failed: {error}",
            ),
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

/// The line for the tasks whose children the drain deadline killed, as
/// `Worker.stop` in `core/worker/worker.py` of the `v0.7.0` package wrote it
/// (`'Drain exceeded %.0fs (hung task?); killed tasks: %s'` with the list of
/// the task ids), or `None` when it killed none: the line needs an id. Pure.
fn drain_kill_line(drain: Duration, killed: &[Uuid]) -> Option<String> {
    match killed.is_empty() {
        true => None,
        false => {
            let ids: Vec<String> = killed
                .iter()
                .map(|task_id| format!("'{task_id}'"))
                .collect();
            Some(format!(
                "Drain exceeded {:.0}s (hung task?); killed tasks: [{}]",
                drain.as_secs_f64(),
                ids.join(", ")
            ))
        }
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
            trace: row.trace,
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

/// What the ping responder needs to answer a ping.
#[derive(Clone)]
struct PingResponder {
    broker: Arc<PostgresBroker>,
    worker_id: String,
    hostname: String,
    pid: i32,
}

/// Answers liveness pings from the worker's ping subscription until a stop
/// request or until the listener stops. Each ping is answered in its own
/// task, so a slow reply does not hold the next ping. Pings sent while the
/// listener reconnected, or dropped by a full queue, are lost; their senders
/// time out.
fn spawn_ping_responder(
    pings: PayloadSubscription,
    responder: PingResponder,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let event = tokio::select! {
                _ = cancel.cancelled() => return,
                event = pings.recv() => event,
            };
            match event {
                PayloadEvent::Payload(payload) => {
                    let responder = responder.clone();
                    let _reply = tokio::spawn(async move {
                        if let Err(error) = handle_worker_ping(
                            &responder.broker,
                            &responder.worker_id,
                            &responder.hostname,
                            responder.pid,
                            &payload,
                        )
                        .await
                        {
                            tracing::error!(error = %error, "Ping responder error: {error}");
                        }
                    });
                }
                PayloadEvent::Gap | PayloadEvent::Overflowed => {
                    tracing::debug!("pings may have been lost; their senders time out");
                }
                PayloadEvent::Stopped => {
                    tracing::warn!("the listener stopped; the ping responder ends");
                    return;
                }
            }
        }
    })
}

/// The listener subscriptions of a running worker.
struct WorkerSubscriptions {
    claim_wakes: ClaimWakes,
    pings: PayloadSubscription,
}

/// The claim wake channel of a queue: `task_queue_<queue>`. A queue name
/// whose channel passes PostgreSQL's 63-byte limit is a configuration error.
fn queue_channel(queue: &str) -> Result<ChannelName, WorkerError> {
    ChannelName::parse(&format!("task_queue_{queue}")).map_err(|refused| {
        WorkerError::Config(format!(
            "queue {queue:?} cannot be listened on: the channel task_queue_{queue}: {refused}"
        ))
    })
}

/// The hook of a host that a worker calls once per run, after its start and
/// before its first claim (Python `run_forever`, `freeze_startup_objects`).
#[derive(Clone)]
pub enum OnStarted {
    /// No hook.
    Nothing,
    /// The host's hook. It must not fail: it handles and logs its own
    /// failure, and the worker runs on.
    Call(Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>),
}

/// Asks a worker to stop, from any thread, before or during its run.
/// Clones ask the same worker.
#[derive(Clone, Default)]
pub struct WorkerStop {
    cancel: CancellationToken,
}

impl WorkerStop {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ask for the stop. Idempotent; returns at once; never blocks.
    pub fn request_stop(&self) {
        self.cancel.cancel();
    }
}

/// Task queue worker.
///
/// Claims tasks from PostgreSQL with the budgets of its children, runs each
/// in a child process of the executor, and writes how each attempt ended.
pub struct Worker {
    broker: Arc<PostgresBroker>,
    /// Where the workflow steps of the worker get the spec of a
    /// sub-workflow child: the parent advances of phase 2, workflow recovery
    /// and phase-2 recovery.
    child_specs: Arc<dyn ChildSpecSource>,
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
    on_started: OnStarted,
    /// Set when the start ended and the claim loop begins.
    serving: AtomicBool,
    /// The metrics and the events of the lifecycle points; off unless the
    /// host turns it on.
    telemetry: WorkerTelemetry,
}

impl Worker {
    /// Create a worker that runs `profile`, the profile that the worker
    /// manifest selects ([`crate::worker_manifest::WorkerManifest::selected_profile`],
    /// [`crate::core::config::worker_profile::select_worker_profile`]).
    /// `launch` names what every child runs; `registered` the class of each
    /// task of the app. The sub-workflow children of the worker's workflow
    /// steps are built by `child_specs`, the child spec source of the client.
    ///
    /// Errors: [`WorkerError::Config`] for a worker config that does not
    /// validate, or a profile that starts no child (AppConfig validation
    /// refuses such a profile; Python `execution_settings`).
    pub(crate) fn of_child_specs(
        broker: Arc<PostgresBroker>,
        child_specs: Arc<dyn ChildSpecSource>,
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
            child_specs,
            app_config,
            worker_config,
            profile,
            launch,
            registered,
            worker_id: Uuid::new_v4().to_string(),
            cancel: CancellationToken::new(),
            hostname,
            on_started: OnStarted::Nothing,
            serving: AtomicBool::new(false),
            telemetry: WorkerTelemetry::Off,
        })
    }

    /// This worker with `on_started` as its start hook.
    pub fn with_on_started(self, on_started: OnStarted) -> Self {
        Self { on_started, ..self }
    }

    /// This worker with `telemetry` at its lifecycle points.
    pub fn with_telemetry(self, telemetry: WorkerTelemetry) -> Self {
        Self { telemetry, ..self }
    }

    /// This worker stopped by `stop`: a request through it, also one made
    /// before the run, ends the run as [`Worker::request_stop`] does.
    pub fn with_stop(self, stop: &WorkerStop) -> Self {
        Self {
            cancel: stop.cancel.clone(),
            ..self
        }
    }

    /// Run the worker ([`Worker::run`]) and name how the run ended. A failed
    /// run's error is logged with its subject and cause before this
    /// returns.
    pub async fn run_to_end(&self) -> WorkerRunEnd {
        let result = self.run().await;
        if let Err(error) = &result {
            tracing::error!("worker run ended: {error}");
        }
        let phase = match self.serving.load(Ordering::Acquire) {
            true => RunPhase::Serving,
            false => RunPhase::Starting,
        };
        run_end_of(result, phase)
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
                        tracing::warn!(
                            error = %e,
                            "failed to register SIGTERM handler: {e}; graceful shutdown via \
                             SIGTERM unavailable",
                        );
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
            orphan_self_heal: OrphanSelfHeal::of_recovery_flag(
                self.app_config
                    .recovery
                    .auto_terminate_orphaned_workflow_tasks,
            ),
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
    /// spawner thread, the preload process of the profile, one pool per
    /// kind, then the wait for the first generation, the placement of the
    /// worker's threads and the memory baseline guard. A stop during the
    /// start ends it with `Ok(None)`.
    ///
    /// Errors: [`WorkerError::PlacementRefused`], [`WorkerError::SpawnerStart`],
    /// [`WorkerError::PreloadRefused`], [`WorkerError::ChildHookFailed`]
    /// (never retried), [`WorkerError::ChildrenStartFailed`]. The pools and
    /// the spawner are stopped before an error returns.
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
        // The preload process starts before the first child, so the first
        // generation forks from it (Python `SlotPool.start`).
        if let Some(plan) = preload_plan(profile, &self.launch) {
            let mode = plan.mode;
            match preload_outcome(mode, spawner.handle().start_preload(plan).await) {
                PreloadOutcome::Forks | PreloadOutcome::Exec { warning: None } => {}
                PreloadOutcome::Exec {
                    warning: Some(warning),
                } => tracing::warn!("{warning}"),
                PreloadOutcome::Refused { reason } => {
                    RunningChildren::stop_spawner(spawner).await;
                    return Err(WorkerError::PreloadRefused { reason });
                }
            }
        }
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

    /// Authorize the schema, check the client family, and ensure the
    /// partition coverage of the current period: the database steps of the
    /// worker start. It does not retry.
    ///
    /// Outcomes: `Ready` when a heartbeat leaf covers the present instant;
    /// `CoverageGateBusy` when the startup coverage found the coverage gate
    /// held by another process and no such leaf. Errors: the schema, family
    /// and coverage errors of the broker; [`WorkerError::Config`] for every
    /// other startup coverage refusal (not retryable).
    async fn start_database(&self) -> Result<DatabaseStart, WorkerError> {
        self.broker.ensure_schema_initialized().await?;
        // The startup coverage and the reaper's partition maintenance are the
        // worker's maintenance writes. The handle checks its client family
        // once, before the first of them; a database of the other family or
        // with no family row refuses the start, not retryable.
        self.broker.ensure_client_family_for_write().await?;
        let declared_periods = self.app_config.retention.registrable_periods();
        let request = CoverageRequest::new(
            self.app_config.retention.history_leaf_horizon_days,
            self.app_config.retention.heartbeat_leaf_horizon_hours,
            &declared_periods,
        )?;
        let coverage = self
            .broker
            .ensure_partition_coverage(&request, CoverageSerialization::Gated)
            .await?;
        match startup_verdict(coverage) {
            StartupCoverageOutcome::Ready(outcome) => {
                tracing::info!(outcome = ?outcome, "task-history startup coverage ready");
                Ok(DatabaseStart::Ready)
            }
            StartupCoverageOutcome::GateBusy(_busy) => Ok(DatabaseStart::CoverageGateBusy),
            StartupCoverageOutcome::Refused(outcome) => Err(WorkerError::Config(format!(
                "task-history startup coverage refused: {outcome:?}"
            ))),
        }
    }

    /// Run [`Worker::start_database`] with each attempt bounded at
    /// [`START_ATTEMPT_TIMEOUT`]. A busy coverage gate, a timed-out attempt
    /// and a retryable error are retried with the resilience backoff, each
    /// wait with one log line ([`StartRetryCause::log_retry`]). A refusal is
    /// returned at once. After the retry budget, a retryable error is
    /// returned, and a busy gate is [`WorkerError::CoverageGateBusy`]. A stop
    /// request during a backoff wait ends the start without an error.
    async fn start_database_with_resilience(&self) -> Result<StartOutcome, WorkerError> {
        let mut backoff = RetryBackoff::from_config(&self.app_config.resilience);
        let first_attempt = std::time::Instant::now();
        loop {
            let cause = match tokio::time::timeout(START_ATTEMPT_TIMEOUT, self.start_database())
                .await
            {
                Ok(Ok(DatabaseStart::Ready)) => return Ok(StartOutcome::Started),
                Ok(Ok(DatabaseStart::CoverageGateBusy)) => StartRetryCause::CoverageGateBusy,
                Ok(Err(error)) if !error.is_retryable() => return Err(error),
                Ok(Err(error)) => StartRetryCause::Failed(Box::new(error)),
                Err(_elapsed) => StartRetryCause::Failed(Box::new(WorkerError::StartTimedOut {
                    bound: START_ATTEMPT_TIMEOUT,
                })),
            };
            if !backoff.can_retry() {
                // The first attempt and one attempt after each retry wait.
                let attempts = backoff.attempts() + 1;
                let error = cause.into_final_error(first_attempt.elapsed(), attempts);
                tracing::error!(
                    %error,
                    attempts,
                    "Worker start failed after {attempts} attempts: {error}",
                );
                return Err(error);
            }
            let wait = Duration::from_secs_f64(backoff.next_delay_seconds());
            cause.log_retry(wait, backoff.attempts());
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(StartOutcome::StopRequested),
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }

    /// The reaper of this worker: the cluster gate, the recovery steps, the
    /// retention cleanup and the fleet check on the worker profile pool,
    /// partition maintenance on the session-work pool.
    fn spawn_reaper_loop(&self, health: ReaperHealth) -> tokio::task::JoinHandle<()> {
        // The reaper's gate and pass check out as reserved work, which the
        // batch users of the worker profile pool cannot exhaust.
        spawn_reaper(
            Arc::clone(&self.broker),
            Arc::clone(&self.child_specs),
            self.app_config.recovery.clone(),
            self.app_config.retention.clone(),
            health,
            self.telemetry.recovery_failures(),
            self.cancel.clone(),
        )
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
    /// Errors: [`WorkerError::Broker`] when the broker's core pools cannot
    /// be built; [`WorkerError::Config`] when its main pool keeps no reserve
    /// (a broker not built with `connect_worker_with`), or leaves no
    /// connection for a flusher.
    fn finalize_batcher(&self) -> Result<FinalizeBatcher<WorkerPoolSuccessWrite>, WorkerError> {
        // The core handle gives its main pool with a write token only; the
        // token ends with this function and no statement runs under it.
        let (open, _write) = self.broker.core_pools_for_write()?;
        let sizing = open.main().sizing();
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
    /// that `hold` checks waits in `hold` with the budget of its route; then
    /// the start check admits the held rows that their queue's count leaves
    /// room for and that a free child of their budget takes now, also on a
    /// pass that claimed nothing. A row of a class that the plan does not
    /// serve is submitted at once: it reaches no child, and its claim goes
    /// back to the queue (`NoChildOfItsKind`), as for an uncapped queue.
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
        let service = children.service();
        let plan = service.plan();
        let request = self.claim_request(service.claim_classes());
        let claimed = claim_for_dispatch(steps, &request, &self.registered).await?;
        for row in claimed.rows {
            match (hold.holds(&row.queue_name), plan.route(row.execution)) {
                (true, ClaimRoute::ServedBy(budget)) => hold.hold(row, budget),
                (false, _) | (true, ClaimRoute::NotServed) => self.dispatch(
                    children,
                    writer,
                    writes,
                    claimed_of(row),
                    StartCount::Unchecked,
                ),
            }
        }
        let admitted = admit_held_rows(&self.broker, &self.worker_id, hold, || {
            children.free_capacity()
        })
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
        let settle = async move {
            let end = attempt_end(submitted).await;
            let settled = writer.settle(&task, end).await;
            tracing::debug!(task_id = %task.task_id, settlement = ?settled, "attempt end settled");
        };
        writes.spawn(settle_then_release(settle, count, released));
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
        claim_wakes: &ClaimWakes,
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
                        "Worker loop error: {error}. Retrying in {delay:.1}s (attempt {})",
                        claim_backoff.attempts(),
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
                        "claim error with no retry left: {error}; shutting down",
                    );
                    return Some(error);
                }
            }
            match wait_for_claim_wake(
                &self.cancel,
                &self.capacity_released,
                children.board(),
                claim_wakes,
                poll_interval,
            )
            .await
            {
                ClaimWake::Cancelled => return None,
                ClaimWake::ListenerClosed => {
                    tracing::info!("the listener stopped, shutting down");
                    return None;
                }
                ClaimWake::DatabaseNotification => {
                    claim_wakes
                        .drain_pending(self.worker_config.coalesce_notifies)
                        .await;
                }
                ClaimWake::ListenerGap
                | ClaimWake::CapacityReleased
                | ClaimWake::ChildReady
                | ClaimWake::PollElapsed => {}
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

        // Check the session URL, then subscribe the claim wakes and the pings
        // on the broker's listener, with resilience.
        let WorkerSubscriptions { claim_wakes, pings } =
            match self.connect_listener_with_resilience().await {
                Ok(subscriptions) => subscriptions,
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

        // The liveness lock is held before the first claim, until every task
        // of the stop ended: a peer that finds it free in two passes far
        // enough apart recovers this worker's rows.
        let liveness_start = self.start_liveness_with_resilience(&live).await;
        let StartedLivenessLock {
            running: liveness,
            holder: liveness_holder,
        } = match liveness_start {
            Ok(Some(started)) => started,
            Ok(None) => {
                RunningChildren {
                    children,
                    spawner,
                    live,
                }
                .stop_after_failed_start()
                .await;
                return Ok(());
            }
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

        // Liveness pings are answered off the claim path.
        let ping_responder = spawn_ping_responder(
            pings,
            PingResponder {
                broker: Arc::clone(&self.broker),
                worker_id: self.worker_id.clone(),
                hostname: self.hostname.clone(),
                pid: std::process::id() as i32,
            },
            self.cancel.clone(),
        );

        let writer = Arc::new(OutcomeWriter::new(
            Arc::clone(&self.broker),
            Arc::clone(&self.child_specs),
            self.worker_id.clone(),
            self.app_config.payload.clone(),
            batcher.clone(),
        ));
        let stops = ChildrenStopCell::default();
        let services =
            match self.spawn_services(&children, &live, &stops, ping_responder, liveness_holder) {
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
                    liveness.stop().await;
                    return Err(error);
                }
            };
        // The start ended: the first generation is in, and no claim ran.
        self.serving.store(true, Ordering::Release);
        match &self.on_started {
            OnStarted::Nothing => {}
            OnStarted::Call(hook) => hook().await,
        }
        tracing::info!(worker_id = %self.worker_id, "worker ready");

        let mut writes: JoinSet<()> = JoinSet::new();
        // Soft-cap mode: the claimed rows of each capped queue wait here
        // until a claim pass finds room under the queue's cap.
        let mut hold = QueueCapHold::new(StartCheckCaps::of_mode(
            self.app_config.prefetch_buffer,
            &self.worker_config.queue_max_concurrency,
        ));
        let fatal_claim_error = self
            .claim_loop(&children, &writer, &mut writes, &mut hold, &claim_wakes)
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
        // Every task ended: the liveness lock is released, so a peer may
        // recover a row that the stop left at its next passes.
        liveness.stop().await;
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

    /// Start the service loops: the claimer heartbeat, the reaper, the
    /// worker snapshot, the runner heartbeats of the tasks that children
    /// hold, the watch of the pool failure and the new-kind baseline guard.
    /// Each of them, `ping_responder` and `liveness_holder` (the holder of
    /// the liveness lock) runs under a watchdog: one that ends
    /// with no stop request stops the worker with `ServiceLoopDied`
    /// (C11-inverse). With stale running recovery off, no task depends on a
    /// runner beat: a beat writer that dies is logged once and the worker
    /// runs on ([`launch_runner_beats`]).
    ///
    /// Errors: [`WorkerError::Config`] when the runner heartbeat pool is
    /// refused, or its watchdog thread does not start.
    fn spawn_services(
        &self,
        children: &WorkerChildren,
        live: &LiveDispatches,
        stops: &ChildrenStopCell,
        ping_responder: JoinHandle<()>,
        liveness_holder: JoinHandle<()>,
    ) -> Result<Services, WorkerError> {
        let claimer_interval =
            Duration::from_millis(self.app_config.recovery.claimer_heartbeat_interval_ms);
        let claimer_hb = spawn_claimer_heartbeat(
            Arc::clone(&self.broker),
            self.worker_id.clone(),
            self.hostname.clone(),
            std::process::id() as i32,
            claimer_interval,
            effective_claim_lease_ms(&self.app_config),
            self.app_config.max_claim_renew_age_ms,
            self.cancel.clone(),
        );
        let reaper_health = new_reaper_health();
        let reaper = self.spawn_reaper_loop(Arc::clone(&reaper_health));
        let state_loop = crate::worker::worker_state::spawn_worker_state_loop(
            Arc::clone(&self.broker),
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
        let beat_pool = runner_beat_pool(&self.app_config.broker).map_err(|error| {
            WorkerError::Config(format!("the runner heartbeat pool is refused: {error}"))
        })?;
        let (runner_beat_watch, runner_beats) = launch_runner_beats(
            Arc::new(beat_pool),
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

        // Supervise the service loops (C11-inverse): a loop that dies without
        // a shutdown request must not leave the worker claiming with lease
        // renewal, stale-task recovery, or liveness advertising dead. The
        // claim wakes need no watchdog: the claim loop reads them, and a
        // stopped listener ends it.
        let (fatal_tx, fatal_rx) =
            mpsc::unbounded_channel::<crate::worker::supervision::ServiceExit>();
        let watched: [(&'static str, tokio::task::JoinHandle<()>); 8] = [
            ("claimer-heartbeat", claimer_hb),
            ("runner-heartbeat", runner_beats),
            ("reaper", reaper),
            ("worker-state", state_loop),
            ("ping-responder", ping_responder),
            ("slot-failure-watch", pool_failure),
            ("new-kind-baseline-watch", new_kind_baselines),
            ("liveness-lock", liveness_holder),
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

    /// Check the pools and the session URL, then subscribe the claim wakes of
    /// every queue and the ping channel on the broker's listener.
    ///
    /// The check runs at every worker boot, with or without PgBouncer mode
    /// (Python `validate_worker_database_paths`): one session check on the
    /// statement layer ([`PostgresBroker::check_worker_session`]). A
    /// statement-pooled URL or a URL that does not keep session state is
    /// refused, not retryable; a database that cannot be reached is
    /// retryable.
    async fn try_connect_listener(&self) -> Result<WorkerSubscriptions, WorkerError> {
        let channels = self
            .worker_config
            .queues
            .iter()
            .map(|queue| queue_channel(queue))
            .collect::<Result<Vec<ChannelName>, WorkerError>>()?;
        self.broker
            .check_worker_session()
            .await
            .map_err(WorkerError::SessionCheck)?;
        let listener = self.broker.listener();
        let mut subscriptions = Vec::with_capacity(channels.len());
        for channel in channels {
            subscriptions.push(
                listener
                    .subscribe_wake(channel, PayloadFilter::AnyPayload)
                    .await
                    .map_err(WorkerError::Listener)?,
            );
        }
        // The global task_new channel is deliberately NOT subscribed: the
        // per-queue channels cover every insert and capacity signal this worker
        // can act on, while task_new wakes every worker for every insert
        // cluster-wide (thundering herd). The trigger still emits task_new for
        // external observers. Parity with horsies PR #101 cafc9200.
        // Subscribe to the shared liveness-ping channel BEFORE any background
        // loop is spawned, so a subscription failure cannot orphan running loops.
        let pings = listener
            .subscribe_payloads(
                fixed_channel(WORKER_PING_CHANNEL),
                default_payload_capacity(),
            )
            .await
            .map_err(WorkerError::Listener)?;
        Ok(WorkerSubscriptions {
            claim_wakes: ClaimWakes { subscriptions },
            pings,
        })
    }

    /// Take the worker's liveness lock and start its holder and watchdog
    /// ([`start_liveness_lock`]) on the session URL, with the resilience
    /// backoff after a failure that can clear, as the listener. A stop
    /// request during a backoff wait ends the start with `Ok(None)`.
    ///
    /// Errors: [`WorkerError::LivenessLock`] for a refused plan or lock, or
    /// for a failure that can clear after the retry budget.
    async fn start_liveness_with_resilience(
        &self,
        live: &LiveDispatches,
    ) -> Result<Option<StartedLivenessLock>, WorkerError> {
        let plan = liveness_connect_plan(&self.app_config.broker)
            .map_err(|refused| WorkerError::LivenessLock(LivenessStartError::Plan(refused)))?;
        let plan = Arc::new(plan);
        let rule = LivenessRule::of_config(&self.app_config.recovery);
        let mut backoff = RetryBackoff::from_config(&self.app_config.resilience);
        loop {
            let error =
                match start_liveness_lock(Arc::clone(&plan), &self.worker_id, rule, live.clone())
                    .await
                {
                    Ok(started) => return Ok(Some(started)),
                    Err(failed) => WorkerError::LivenessLock(failed),
                };
            if !error.is_retryable() {
                tracing::error!(error = %error, "non-retryable liveness lock error: {error}");
                return Err(error);
            }
            if !backoff.can_retry() {
                tracing::error!(
                    error = %error,
                    attempts = backoff.attempts(),
                    "liveness lock retries exhausted: {error}",
                );
                return Err(error);
            }
            let delay = backoff.next_delay_seconds();
            tracing::warn!(
                error = %error,
                attempt = backoff.attempts(),
                delay_s = format!("{:.2}", delay),
                "taking the liveness lock failed: {error}; retrying",
            );
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(None),
                _ = tokio::time::sleep(Duration::from_secs_f64(delay)) => {}
            }
        }
    }

    /// Run [`Worker::try_connect_listener`] with exponential backoff on
    /// transient failures.
    async fn connect_listener_with_resilience(&self) -> Result<WorkerSubscriptions, WorkerError> {
        let mut backoff = RetryBackoff::from_config(&self.app_config.resilience);

        loop {
            match self.try_connect_listener().await {
                Ok(listener) => return Ok(listener),
                Err(e) => {
                    if !e.is_retryable() {
                        tracing::error!(error = %e, "non-retryable listener connection error: {e}");
                        return Err(e);
                    }
                    if !backoff.can_retry() {
                        tracing::error!(
                            error = %e,
                            attempts = backoff.attempts(),
                            "listener connection retries exhausted: {e}",
                        );
                        return Err(e);
                    }
                    let delay = backoff.next_delay_seconds();
                    tracing::warn!(
                        error = %e,
                        attempt = backoff.attempts(),
                        delay_s = format!("{:.2}", delay),
                        "listener connection failed: {e}; retrying",
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
    use crate::test_support::sql::{self, TestListener, TestPool, TestPoolOptions};
    use crate::worker::execution::{
        capacity_wake, persist_ok_result, persist_terminal_state, run_phase2, send_phase2_wake,
        FinalizeOutcome, FinalizeStage, Phase2Work,
    };

    use crate::broker::PostgresBroker;
    use crate::core::config::recovery::RecoveryConfig;
    use crate::core::history::maintenance::coverage::StartupCoverageOutcome;
    use crate::core::task::fn_trait::{AsyncTaskFn, RawTaskResult, RegisteredTask, TaskMeta};
    use crate::core::task::{OperationalErrorCode, TaskError, TaskErrorCode, TaskResult, UserCode};
    use chrono::Utc;
    use futures::FutureExt;
    use serial_test::serial;
    use std::future::Future;
    use std::panic::{resume_unwind, AssertUnwindSafe};
    use std::pin::Pin;
    use std::sync::Arc;
    use uuid::Uuid;

    /// Claim wakes with no queue subscription: the wake branch never fires.
    fn no_claim_wakes() -> ClaimWakes {
        ClaimWakes {
            subscriptions: Vec::new(),
        }
    }

    /// A board of one isolated slot whose child never reports ready: its
    /// child-ready wake fires only when a test notes one.
    fn quiet_board() -> PoolBoard {
        PoolBoard::new(crate::worker::executor::slots::ChildLayout {
            isolated: 1,
            shared_async: 0,
            shared_thread: 0,
        })
    }

    /// A worker on `broker` of one isolated child that boots, then exits at
    /// the next frame it reads.
    fn worker_of_one_idle_child(
        broker: Arc<PostgresBroker>,
        app_config: AppConfig,
        worker_config: WorkerConfig,
    ) -> Worker {
        worker_of_idle_children(broker, app_config, worker_config, 1)
    }

    /// A worker on `broker` of `count` isolated children, each of which
    /// boots, then exits at the next frame it reads.
    fn worker_of_idle_children(
        broker: Arc<PostgresBroker>,
        app_config: AppConfig,
        worker_config: WorkerConfig,
        count: u32,
    ) -> Worker {
        use crate::worker::executor::protocol::{
            ChildConnection, ChildLogLevel, ChildProgram, DatabaseUrl,
        };
        let database_url = DatabaseUrl::new(app_config.broker.database_url.clone());
        Worker::of_child_specs(
            broker,
            crate::test_support::child_specs::no_child_specs(),
            app_config,
            worker_config,
            SelectedProfile {
                name: "default".to_owned(),
                profile: serde_json::from_value(serde_json::json!({
                    "isolated": {"kind": "isolated_children", "count": count}
                }))
                .expect("a valid test profile"),
            },
            ChildLaunch {
                command: crate::worker::executor::slots::scripted::idle_child_of_any_kind(),
                preload: crate::worker::children::PreloadProgram::NoPreloadProgram,
                program: ChildProgram::Rust,
                log_level: ChildLogLevel::Info,
                database_url,
                connection: ChildConnection::default(),
            },
            RegisteredClasses::default(),
        )
        .expect("a valid test worker")
    }

    #[test]
    fn a_claim_wake_follows_the_listener_event() {
        assert_eq!(
            ClaimWake::from_listener(WakeEvent::Notified),
            ClaimWake::DatabaseNotification
        );
        assert_eq!(
            ClaimWake::from_listener(WakeEvent::Gap),
            ClaimWake::ListenerGap
        );
        assert_eq!(
            ClaimWake::from_listener(WakeEvent::Stopped),
            ClaimWake::ListenerClosed
        );
    }

    /// `task_queue_` is 11 bytes, so a queue name of 52 bytes fits the
    /// 63-byte channel limit and 53 bytes does not.
    #[test]
    fn a_queue_whose_channel_does_not_fit_is_a_config_error() {
        assert!(queue_channel(&"q".repeat(52)).is_ok());
        let refused = queue_channel(&"q".repeat(53)).expect_err("the channel is too long");
        assert!(matches!(refused, WorkerError::Config(_)));
        assert!(!refused.is_retryable());
    }

    /// A broker with no listener cannot carry claim wakes or pings: the
    /// worker refuses to start, not retryable, before any database call.
    #[tokio::test]
    async fn a_worker_on_a_broker_without_a_listener_refuses_to_start() {
        let pool = TestPoolOptions::new().connect_lazy("postgresql://horsies@127.0.0.1:1/horsies");
        let worker = worker_of_one_idle_child(
            Arc::new(PostgresBroker::from_test_url(
                pool.url(),
                crate::ClientFamily::Rust,
            )),
            AppConfig::for_database_url("postgresql://horsies@127.0.0.1:1/horsies"),
            WorkerConfig::default(),
        );

        let Err(refused) = worker.try_connect_listener().await else {
            panic!("a broker without a listener cannot serve a worker");
        };

        assert!(
            matches!(
                refused,
                WorkerError::SessionCheck(crate::broker::SessionCheckFailure::Subscribe(
                    crate::broker::ListenError::NoSessionUrl
                ))
            ),
            "{refused}"
        );
        assert!(!refused.is_retryable());
    }

    /// The boot check passes on a direct URL, and the claim wakes and the
    /// ping replies both travel over the broker's one listener.
    #[tokio::test]
    #[serial]
    async fn a_booted_worker_wakes_on_its_queue_and_answers_pings() {
        let broker = Arc::new(PostgresBroker::from_test_url_and_session_url(
            test_pool().await.url(),
            &crate::test_support::migrated_database_url().await,
            crate::ClientFamily::Rust,
        ));
        let worker = worker_of_one_idle_child(
            Arc::clone(&broker),
            AppConfig::for_database_url("postgresql://horsies@127.0.0.1:1/horsies"),
            WorkerConfig {
                queues: vec!["listener_boot_q".to_owned()],
                ..WorkerConfig::default()
            },
        );
        let WorkerSubscriptions { claim_wakes, pings } = worker
            .try_connect_listener()
            .await
            .expect("the session check passes and the subscriptions start");

        sql::query("SELECT pg_notify('task_queue_listener_boot_q', 'wake')")
            .execute(&test_pool().await)
            .await
            .expect("notify the queue");
        let wake = tokio::time::timeout(
            Duration::from_secs(5),
            wait_for_claim_wake(
                &worker.cancel,
                &worker.capacity_released,
                &quiet_board(),
                &claim_wakes,
                Duration::from_secs(60),
            ),
        )
        .await
        .expect("the queue notification wakes the claim loop");
        assert_eq!(wake, ClaimWake::DatabaseNotification);

        let responder = spawn_ping_responder(
            pings,
            PingResponder {
                broker: Arc::clone(&broker),
                worker_id: worker.worker_id().to_owned(),
                hostname: "listener-boot-host".to_owned(),
                pid: 1,
            },
            worker.cancel.clone(),
        );
        let pongs = broker
            .ping_workers(Some(worker.worker_id()), Duration::from_secs(5), None)
            .await
            .expect("the ping runs");
        assert_eq!(pongs.len(), 1, "{pongs:?}");
        assert_eq!(pongs[0].worker_id, worker.worker_id());

        worker.cancel.cancel();
        responder
            .await
            .expect("the ping responder ends at the stop request");
    }

    /// The end of an outcome write wakes the claim loop before its poll
    /// deadline (Python `_capacity_released`, set by each finalizer).
    #[tokio::test]
    async fn released_capacity_wakes_claim_wait_before_poll_deadline() {
        let released = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let claim_wakes = no_claim_wakes();
        let board = quiet_board();
        let wait = wait_for_claim_wake(
            &cancel,
            &released,
            &board,
            &claim_wakes,
            Duration::from_secs(60),
        );
        tokio::pin!(wait);
        assert!(wait.as_mut().now_or_never().is_none());
        released.notify_one();
        let wake = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("local capacity must wake the claim loop before polling");
        assert_eq!(wake, ClaimWake::CapacityReleased);
    }

    /// Python `test_worker_helpers.py::TestCapacityReleaseWake::test_finalizer_end_sets_the_wake`:
    /// the write of the end of a dispatched run wakes the claim loop when it
    /// ends, and not before. The write is the one step of every end of a run
    /// (a success, a failure, a run that reached no child), so each of them
    /// frees its slot or seat for the next pass.
    #[tokio::test]
    async fn the_end_of_an_outcome_write_wakes_the_claim_wait() {
        let released = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let claim_wakes = no_claim_wakes();
        let board = quiet_board();
        let (end_the_write, write_ended) = tokio::sync::oneshot::channel::<()>();
        let write = tokio::spawn(settle_then_release(
            async move {
                let _ended = write_ended.await;
            },
            StartCount::Unchecked,
            Arc::clone(&released),
        ));
        let wait = wait_for_claim_wake(
            &cancel,
            &released,
            &board,
            &claim_wakes,
            Duration::from_secs(60),
        );
        tokio::pin!(wait);
        tokio::task::yield_now().await;
        assert!(
            wait.as_mut().now_or_never().is_none(),
            "no wake while the write runs"
        );

        end_the_write.send(()).expect("the write waits for its end");
        let wake = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("the end of the write must wake the claim loop before polling");

        assert_eq!(wake, ClaimWake::CapacityReleased);
        write.await.expect("the write task ends");
    }

    /// A write that ends during a claim pass leaves the wake set, so the
    /// next wait returns at once (Python: the loop clears the event before
    /// each pass).
    #[tokio::test]
    async fn released_capacity_is_retained_before_claim_wait_starts() {
        let released = Arc::new(Notify::new());
        released.notify_one();
        let cancel = CancellationToken::new();
        let claim_wakes = no_claim_wakes();
        let board = quiet_board();
        let wake = tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_claim_wake(
                &cancel,
                &released,
                &board,
                &claim_wakes,
                Duration::from_secs(60),
            ),
        )
        .await
        .expect("release before waiting must not lose the wake");
        assert_eq!(wake, ClaimWake::CapacityReleased);
        assert!(
            wait_for_claim_wake(
                &cancel,
                &released,
                &board,
                &claim_wakes,
                Duration::from_secs(60),
            )
            .now_or_never()
            .is_none(),
            "an idle worker must not keep waking"
        );
    }

    /// #1148: a child ready after a boot wakes the claim loop before its
    /// poll deadline, once. Ready children noted while the loop ran a pass
    /// wake the next wait once, not once each.
    #[tokio::test]
    async fn a_child_ready_after_a_boot_wakes_the_claim_wait_once() {
        let released = Notify::new();
        let cancel = CancellationToken::new();
        let claim_wakes = no_claim_wakes();
        let board = quiet_board();
        let wait = wait_for_claim_wake(
            &cancel,
            &released,
            &board,
            &claim_wakes,
            Duration::from_secs(60),
        );
        tokio::pin!(wait);
        assert!(wait.as_mut().now_or_never().is_none());
        board.note_child_ready();
        let wake = tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("a ready child must wake the claim loop before polling");
        assert_eq!(wake, ClaimWake::ChildReady);

        board.note_child_ready();
        board.note_child_ready();
        let wake = tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_claim_wake(
                &cancel,
                &released,
                &board,
                &claim_wakes,
                Duration::from_secs(60),
            ),
        )
        .await
        .expect("a ready child noted before the wait must not lose the wake");
        assert_eq!(wake, ClaimWake::ChildReady);
        assert!(
            wait_for_claim_wake(
                &cancel,
                &released,
                &board,
                &claim_wakes,
                Duration::from_secs(60),
            )
            .now_or_never()
            .is_none(),
            "two ready children noted during one pass wake one wait"
        );
    }

    async fn test_pool() -> TestPool {
        let pool = crate::test_support::migrated_pool().await;
        let core = crate::broker::pools::core_pool_for_test_url(pool.url());
        let coverage =
            crate::core::history::maintenance::coverage::ensure_startup_coverage_in_pool(
                &core,
                crate::broker::core_rows::StatementLog::Off,
                2,
                2,
                &[],
            )
            .await
            .expect("startup coverage");
        core.close();
        assert!(matches!(coverage, StartupCoverageOutcome::Ready(_)));
        pool
    }

    async fn clean(pool: &TestPool) {
        sql::query(
            "TRUNCATE horsies_task_attempts, horsies_workflow_tasks, horsies_workflows, \
             horsies_tasks, horsies_heartbeats, horsies_worker_states, horsies_schedule_state CASCADE",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn test_broker() -> Arc<PostgresBroker> {
        Arc::new(PostgresBroker::from_test_url(
            test_pool().await.url(),
            crate::ClientFamily::Rust,
        ))
    }

    /// A broker on a loopback port where no server listens: every statement
    /// of its core pools fails at the checkout with a refused connection,
    /// which is retryable. The phase-1 writes run on the core pools, and
    /// `PostgresBroker::close` leaves those pools to reopen at the next
    /// statement.
    fn unreachable_broker() -> PostgresBroker {
        let pool = TestPoolOptions::new().connect_lazy("postgresql://horsies@127.0.0.1:1/horsies");
        PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust)
    }

    /// The claim generation of every row that `insert_claimed_task` seeds and
    /// of every claimed task that `claimed_task` builds, so a test's dispatch
    /// fences on the generation its row carries.
    fn test_claim_generation() -> chrono::DateTime<Utc> {
        static GENERATION: std::sync::OnceLock<chrono::DateTime<Utc>> = std::sync::OnceLock::new();
        *GENERATION.get_or_init(Utc::now)
    }

    async fn insert_claimed_task(
        pool: &TestPool,
        task_id: &Uuid,
        queue_name: &str,
        retry_count: i32,
        max_retries: i32,
        task_options: Option<&str>,
    ) {
        sql::query(
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
        pool: &TestPool,
        task_id: &Uuid,
        queue_name: &str,
        retry_count: i32,
        max_retries: i32,
        task_options: Option<&str>,
    ) {
        sql::query(
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

    /// The claimed task of a plain task seeded by `insert_claimed_task` /
    /// `insert_running_task`, as the claim returns it: the claim generation
    /// of the seed, and the retry facts that the seed stored.
    fn claimed_task(
        task_id: &Uuid,
        queue_name: &str,
        task_options: Option<&str>,
        retry: RetryFacts,
    ) -> ClaimedTask {
        ClaimedTask {
            task_id: *task_id,
            task_name: "finalize_test".to_owned(),
            queue_name: queue_name.to_owned(),
            claimed_at: test_claim_generation(),
            execution: crate::core::task::Execution::SharedAsync,
            kind: TaskKind::Plain,
            retry,
            options: crate::worker::retry::read_stored_options(task_options),
            trace: crate::events::RunTrace::NoCarrier,
        }
    }

    async fn link_task_to_workflow(
        pool: &TestPool,
        task_id: &Uuid,
        output_task_index: Option<i32>,
    ) -> Uuid {
        let workflow_id = Uuid::new_v4();
        let workflow_task_id = Uuid::new_v4();

        sql::query(
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

        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, task_id,
                created_at, max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'node_0', 'finalize_test', 'null', 'default', 100, '{}', FALSE,
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
        sql::query(
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

    /// The status, result and error code of a task. A live row holds no
    /// result and no error code (baseline `horsies_tasks`,
    /// `0001_tables.sql:59-139`): they are on the history row that the move
    /// writes. The move deletes the live row in the same statement, so a task
    /// is in exactly one of the two tables; two rows or none fail the test.
    async fn fetch_task_state(
        pool: &TestPool,
        task_id: &Uuid,
    ) -> (String, Option<String>, Option<String>) {
        let states: Vec<(String, Option<String>, Option<String>)> = sql::query_as(
            "SELECT status, NULL::text AS result, NULL::text AS error_code
             FROM horsies_tasks WHERE id = $1
             UNION ALL
             SELECT status, convert_from(result_payload, 'UTF8') AS result, error_code
             FROM horsies_task_history WHERE task_id = $1",
        )
        .bind(*task_id)
        .fetch_all(pool)
        .await
        .unwrap();
        match <[_; 1]>::try_from(states) {
            Ok([state]) => state,
            Err(states) => panic!(
                "task {task_id} must be in exactly one of horsies_tasks and \
                 horsies_task_history: {states:?}"
            ),
        }
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

    async fn fetch_workflow_state(pool: &TestPool, workflow_id: &Uuid) -> (String, Option<String>) {
        sql::query_as("SELECT status, result FROM horsies_workflows WHERE id = $1")
            .bind(*workflow_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn fetch_workflow_task_status(
        pool: &TestPool,
        workflow_id: &Uuid,
        task_id: &Uuid,
    ) -> String {
        sql::query_scalar(
            "SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_id = $2",
        )
        .bind(*workflow_id)
        .bind(*task_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn fetch_only_workflow_task_state(
        pool: &TestPool,
        workflow_id: &Uuid,
    ) -> (String, Option<Uuid>) {
        sql::query_as(
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

    async fn fetch_attempt_count(pool: &TestPool, task_id: &Uuid) -> i64 {
        sql::query_scalar("SELECT COUNT(*) FROM horsies_task_attempts WHERE task_id = $1")
            .bind(*task_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn install_fail_workflow_task_running_trigger(pool: &TestPool) -> (String, String) {
        let suffix = Uuid::new_v4().simple().to_string();
        let function_name = format!("horsies_test_fail_wf_running_{}", suffix);
        let trigger_name = format!("horsies_test_fail_wf_running_trigger_{}", suffix);

        sql::query(&format!(
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

        sql::query(&format!(
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

    async fn remove_test_trigger(pool: &TestPool, function_name: &str, trigger_name: &str) {
        sql::query(&format!(
            "DROP TRIGGER IF EXISTS {trigger_name} ON horsies_workflow_tasks"
        ))
        .execute(pool)
        .await
        .unwrap();

        sql::query(&format!("DROP FUNCTION IF EXISTS {function_name}()"))
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

    /// Python `clients/python/horsies/core/cli.py:886` and `:955`: the memory fit of the
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

    // -- the worker on the executor --

    /// A worker of one isolated child on a broker whose pool reaches no
    /// server: any database work fails, so a test sees whether some ran.
    fn worker_of_one_child(command: crate::worker::executor::supervisor::ChildCommand) -> Worker {
        worker_of_profile(
            serde_json::json!({
                "isolated": {"kind": "isolated_children", "count": 1}
            }),
            command,
        )
    }

    /// A worker of the profile `profile` whose children run `command`, on a
    /// broker that reaches no server.
    fn worker_of_profile(
        profile: serde_json::Value,
        command: crate::worker::executor::supervisor::ChildCommand,
    ) -> Worker {
        use crate::worker::executor::protocol::{
            ChildConnection, ChildLogLevel, ChildProgram, DatabaseUrl,
        };
        let url = "postgresql://horsies@127.0.0.1:1/horsies";
        let pool = TestPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy(url);
        Worker::of_child_specs(
            Arc::new(PostgresBroker::from_test_url(
                pool.url(),
                crate::ClientFamily::Rust,
            )),
            crate::test_support::child_specs::no_child_specs(),
            AppConfig::for_database_url(url),
            WorkerConfig::default(),
            SelectedProfile {
                name: "default".to_owned(),
                profile: serde_json::from_value(profile).expect("a valid test profile"),
            },
            ChildLaunch {
                command,
                preload: crate::worker::children::PreloadProgram::NoPreloadProgram,
                program: ChildProgram::Rust,
                log_level: ChildLogLevel::Info,
                database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
                connection: ChildConnection::default(),
            },
            RegisteredClasses::default(),
        )
        .expect("a valid test worker")
    }

    #[tokio::test]
    async fn a_worker_with_no_child_is_refused() {
        use crate::worker::executor::protocol::{
            ChildConnection, ChildLogLevel, ChildProgram, DatabaseUrl,
        };
        let url = "postgresql://horsies@127.0.0.1:1/horsies";
        let pool = TestPoolOptions::new().connect_lazy(url);
        let refused = Worker::of_child_specs(
            Arc::new(PostgresBroker::from_test_url(
                pool.url(),
                crate::ClientFamily::Rust,
            )),
            crate::test_support::child_specs::no_child_specs(),
            AppConfig::for_database_url(url),
            WorkerConfig::default(),
            SelectedProfile {
                name: "empty".to_owned(),
                profile: crate::core::config::worker_profile::WorkerProfile::default(),
            },
            ChildLaunch {
                command: crate::worker::executor::slots::scripted::idle_child_of_any_kind(),
                preload: crate::worker::children::PreloadProgram::NoPreloadProgram,
                program: ChildProgram::Rust,
                log_level: ChildLogLevel::Info,
                database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
                connection: ChildConnection::default(),
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

    /// Python `test_worker_helpers.py::TestSlotPoolSetup::test_start_failure_raises_and_keeps_the_pool_for_cleanup`:
    /// a pool that cannot start its first child ends the start with the
    /// pool failure. The core start stops the pools and the spawner thread
    /// itself before it returns the error
    /// (`RunningChildren::stop_after_failed_start`), so nothing is left for
    /// the caller to clean; the start returning within its bound shows the
    /// stop ended.
    #[tokio::test]
    async fn a_child_that_cannot_start_ends_the_start_with_the_pool_failure() {
        use crate::worker::executor::supervisor::{BootFailureCause, ChildCommand};

        let worker = worker_of_one_child(ChildCommand {
            program: "/nonexistent/horsies-child".into(),
            args: Vec::new(),
        });
        let started = tokio::time::timeout(Duration::from_secs(60), worker.start_pools())
            .await
            .expect("the start ends within the bound");
        match started {
            Err(WorkerError::ChildrenStartFailed(ChildrenStartFailed::Pool(
                PoolFailure::BootAttemptsExhausted { last, .. },
            ))) => assert_eq!(last.cause, BootFailureCause::SpawnFailed),
            Err(other) => panic!("another error: {other}"),
            Ok(_) => panic!("a child that cannot start must fail the start"),
        }
    }

    /// Python `test_worker_helpers.py::TestStopDuringStart::test_a_start_that_ends_stopped_still_stops_the_children`:
    /// a stop while the first child boots ends the start with no error, and
    /// the child is stopped and reaped before the start returns. The child
    /// writes its pid and never answers ready, so the start is still waiting
    /// when the stop comes.
    #[tokio::test]
    async fn a_start_that_ends_stopped_still_stops_its_children() {
        use crate::worker::executor::supervisor::ChildCommand;

        let dir =
            std::env::temp_dir().join(format!("horsies-stopped-start-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).expect("create the test folder");
        let pid_file = dir.join("child.pid");
        let worker = worker_of_one_child(ChildCommand {
            program: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                format!("echo $$ > '{}'; exec sleep 60", pid_file.display()).into(),
            ],
        });
        let stop_after_spawn = async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            while !pid_file.exists() && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            worker.request_stop();
        };

        let (started, ()) = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(worker.start_pools(), stop_after_spawn)
        })
        .await
        .expect("the start ends within the bound");

        match started {
            Ok(None) => {}
            Ok(Some(_)) => panic!("the start ended before the stop"),
            Err(error) => panic!("a stop during the start is no failure: {error}"),
        }
        let pid: i32 = std::fs::read_to_string(&pid_file)
            .expect("the child wrote its pid")
            .trim()
            .parse()
            .expect("the pid file holds a pid");
        // SAFETY: signal 0 sends nothing; it only checks that the process
        // exists.
        let exists = unsafe { libc::kill(pid, 0) } == 0;
        assert!(!exists, "the child {pid} still runs after the start ended");
        std::fs::remove_dir_all(&dir).expect("remove the test folder");
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

    /// Python `test_worker_helpers.py::TestSlotFailureLoop::test_a_task_loop_failure_at_boot_names_its_cause`:
    /// replacement children that cannot create their task loop stop the
    /// worker with an error that names the cause and its detail.
    #[tokio::test]
    async fn a_task_loop_failure_at_boot_names_its_cause() {
        use crate::worker::executor::slots::ChildLayout;
        use crate::worker::executor::supervisor::{BootFailure, BootFailureCause};

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
        board.fail(PoolFailure::BootAttemptsExhausted {
            attempts: 3,
            last: BootFailure::new(
                BootFailureCause::TaskLoopCreateFailed,
                "OSError: [Errno 24] Too many open files",
            ),
        });
        tokio::time::timeout(Duration::from_secs(5), watch)
            .await
            .expect("the watch ends at the failure")
            .expect("the watch joins");

        assert!(cancel.is_cancelled());
        let stopped = Worker::stop_result(stopped_services(), stops, None)
            .expect_err("a pool that cannot start a child stops the worker with an error");
        assert!(matches!(stopped, WorkerError::PoolFailed(_)), "{stopped:?}");
        assert!(!stopped.is_retryable());
        assert!(
            stopped.to_string().contains(
                "the child could not create the event loop for async tasks: \
                 OSError: [Errno 24] Too many open files"
            ),
            "{stopped}"
        );
    }

    /// The start of a worker of one isolated child, or two, whose children
    /// report `readings` as their RSS by boot order, with the memory limit
    /// `limit_mb` (`None`: no limit) on the isolated children. A start that
    /// passes is stopped before this returns.
    async fn start_with_baselines(
        limit_mb: Option<u32>,
        readings: &[crate::worker::executor::protocol::RssReading],
    ) -> Result<(), WorkerError> {
        let dir = std::env::temp_dir().join(format!(
            "horsies-baseline-start-{}",
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("create the test folder");
        let above = match limit_mb {
            Some(mb) => serde_json::json!({"kind": "memory_limit", "mb": mb}),
            None => serde_json::json!({"kind": "no_memory_limit"}),
        };
        let worker = worker_of_profile(
            serde_json::json!({
                "isolated": {"kind": "isolated_children", "count": readings.len(),
                             "recycle": {"above": above}}
            }),
            crate::worker::executor::slots::scripted::idle_children_reporting_rss(&dir, readings),
        );
        let started = tokio::time::timeout(Duration::from_secs(60), worker.start_pools())
            .await
            .expect("the start ends within the bound");
        let ended = match started {
            Ok(Some(running)) => {
                running.stop_after_failed_start().await;
                Ok(())
            }
            Ok(None) => panic!("the start ended with no stop request"),
            Err(error) => Err(error),
        };
        std::fs::remove_dir_all(&dir).expect("remove the test folder");
        ended
    }

    /// An RSS reading of `mb` MiB.
    fn rss_mb(mb: f64) -> crate::worker::executor::protocol::RssReading {
        crate::worker::executor::protocol::RssReading::Mb(
            crate::worker::executor::protocol::RssMb::new(mb).expect("a finite RSS"),
        )
    }

    /// Python `test_worker_helpers.py::TestMemoryBaselineGuard::test_no_threshold_skips_the_guard`,
    /// the worker side: with no memory limit in the profile, a large
    /// baseline starts.
    #[tokio::test]
    async fn the_start_with_no_memory_limit_skips_the_baseline_guard() {
        let started = start_with_baselines(None, &[rss_mb(9_999.0)]).await;
        assert!(started.is_ok(), "{started:?}");
    }

    /// Python `TestMemoryBaselineGuard::test_baseline_at_the_threshold_is_refused`,
    /// the worker side: the largest baseline of the first children, at the
    /// limit of the profile, refuses the start.
    #[tokio::test]
    async fn the_start_refuses_a_baseline_at_the_memory_limit() {
        let started = start_with_baselines(Some(100), &[rss_mb(50.0), rss_mb(100.0)]).await;
        match started {
            Err(WorkerError::ChildrenStartFailed(ChildrenStartFailed::BaselineOverLimit(over))) => {
                assert_eq!(
                    over.class,
                    crate::worker::executor::slots::ExecutionClass::Isolated
                );
                assert_eq!(over.baseline_mb, 100.0);
                assert!(over.to_string().contains("100MB"), "{over}");
            }
            other => panic!("expected the baseline refusal, got {other:?}"),
        }
    }

    /// Python `TestMemoryBaselineGuard::test_baseline_below_the_threshold_passes`,
    /// the worker side.
    #[tokio::test]
    async fn the_start_takes_a_baseline_below_the_memory_limit() {
        let started = start_with_baselines(Some(100), &[rss_mb(10.0)]).await;
        assert!(started.is_ok(), "{started:?}");
    }

    /// Python `TestMemoryBaselineGuard::test_unreadable_rss_warns_and_passes`,
    /// the worker side: a baseline that the child could not read skips the
    /// guard with a warning, and the start passes.
    #[tokio::test]
    async fn the_start_with_an_unreadable_baseline_warns_and_passes() {
        let capture = crate::test_support::LogCapture::new();
        let guard = capture.set_default();
        let started = start_with_baselines(
            Some(100),
            &[crate::worker::executor::protocol::RssReading::Unreadable],
        )
        .await;
        drop(guard);

        assert!(started.is_ok(), "{started:?}");
        let warnings: Vec<String> = capture
            .events()
            .into_iter()
            .filter(|event| event.level == tracing::Level::WARN)
            .map(|event| event.message)
            .collect();
        assert!(
            warnings
                .iter()
                .any(|message| message.contains("unreadable")),
            "{warnings:?}"
        );
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
            trace: crate::events::RunTrace::NoCarrier,
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
                trace: crate::events::RunTrace::NoCarrier,
            }
        );
        assert_eq!(claimed.input.as_str(), r#"{"n": 1}"#);
    }

    /// The stop order: the claim loop ends, then the pools stop with the
    /// drain timeout, then the rows held for their queue cap go back to the
    /// queue, then the outcome writes drain and the batcher
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

    /// Python `Worker.stop`: the drain line names each task that the drain
    /// deadline killed, as Python prints its list of ids; a drain that
    /// killed none logs no line.
    /// Python `SlotPool._start_preload`: only the `app` mode refuses; the
    /// `library` mode and a host without the preload path use `exec`.
    #[test]
    fn only_an_app_preload_that_does_not_start_refuses_the_worker() {
        use crate::worker::executor::protocol::ProcessId;
        let ready = PreloadStart::Ready {
            pid: ProcessId::new(42).unwrap(),
        };
        assert_eq!(
            preload_outcome(PreloadMode::App, ready),
            PreloadOutcome::Forks
        );
        for (mode, start) in [
            (
                PreloadMode::App,
                PreloadStart::Refused {
                    reason: "import failed".to_owned(),
                },
            ),
            (
                PreloadMode::App,
                PreloadStart::StartFailed {
                    detail: "import failed".to_owned(),
                },
            ),
        ] {
            assert_eq!(
                preload_outcome(mode, start),
                PreloadOutcome::Refused {
                    reason: "import failed".to_owned()
                }
            );
        }
        assert_eq!(
            preload_outcome(
                PreloadMode::Library,
                PreloadStart::Refused {
                    reason: "import failed".to_owned()
                }
            ),
            PreloadOutcome::Exec {
                warning: Some(
                    "No preload parent, using exec: the preload process refused: import failed"
                        .to_owned()
                )
            }
        );
        let not_linux =
            || PreloadStart::Unsupported(PreloadUnsupported::NotLinux { platform: "macos" });
        assert_eq!(
            preload_outcome(PreloadMode::Library, not_linux()),
            PreloadOutcome::Exec { warning: None }
        );
        assert_eq!(
            preload_outcome(PreloadMode::App, not_linux()),
            PreloadOutcome::Exec {
                warning: Some("App preload needs Linux, this is macos. Using exec.".to_owned())
            }
        );
        // Python `TestFallbackLog::test_a_linux_host_that_cannot_preload_warns`:
        // a Linux host without the preload path warns in both modes.
        for mode in [PreloadMode::App, PreloadMode::Library] {
            assert_eq!(
                preload_outcome(
                    mode,
                    PreloadStart::Unsupported(PreloadUnsupported::MissingKernelSupport {
                        reason: "pidfd_open: ENOSYS".to_owned()
                    })
                ),
                PreloadOutcome::Exec {
                    warning: Some(
                        "Child preload cannot run on this host: pidfd_open: ENOSYS. Using exec."
                            .to_owned()
                    )
                }
            );
        }
    }

    #[test]
    fn the_drain_line_names_the_killed_tasks_as_python_does() {
        let killed = [Uuid::from_u128(1), Uuid::from_u128(2)];

        assert_eq!(
            drain_kill_line(Duration::from_secs(30), &killed).as_deref(),
            Some(
                "Drain exceeded 30s (hung task?); killed tasks: \
                 ['00000000-0000-0000-0000-000000000001', \
                 '00000000-0000-0000-0000-000000000002']"
            )
        );
        assert_eq!(drain_kill_line(Duration::from_secs(30), &[]), None);
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

    /// A task of the tests: the async function `body` of no input, whose
    /// value the task ends with as JSON, or whose error it ends with.
    struct TestTask<F>(F);

    impl<F, Fut> AsyncTaskFn for TestTask<F>
    where
        F: Fn(()) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, TaskError>> + Send + 'static,
    {
        fn execute(
            &self,
            _args: &[u8],
        ) -> Pin<Box<dyn Future<Output = RawTaskResult> + Send + '_>> {
            Box::pin(async move {
                match (self.0)(()).await {
                    Ok(value) => TaskResult::Ok(
                        serde_json::to_vec(&value).expect("a text value encodes as JSON"),
                    ),
                    Err(error) => TaskResult::Err(error),
                }
            })
        }
    }

    /// The registered task of `body` (see [`TestTask`]), defined in this
    /// module: the module that a client's task macro names
    /// ([`TaskMeta::in_module`] with `module_path!()`), so the exception
    /// record of a panic carries it.
    fn test_task<F, Fut>(body: F) -> RegisteredTask
    where
        F: Fn(()) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, TaskError>> + Send + 'static,
    {
        RegisteredTask::Async {
            task: Arc::new(TestTask(body)),
            meta: TaskMeta::for_input::<()>().in_module(module_path!()),
        }
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

    /// The claimed row of `task_id` as the claim returns it, read from the
    /// seeded row. The in-process run is an isolated run: a row of another
    /// class is a defect of the test.
    async fn claimed_from_database(pool: &TestPool, task_id: Uuid) -> Claimed {
        #[allow(clippy::type_complexity)]
        let (
            task_name,
            queue_name,
            input,
            task_options,
            execution,
            is_workflow_task,
            claimed_at,
            retry_count,
            max_retries,
        ): (
            String,
            String,
            String,
            Option<String>,
            String,
            bool,
            chrono::DateTime<Utc>,
            i32,
            i32,
        ) = sql::query_as(
            "SELECT task_name, queue_name, input::text, task_options::text, execution::text,
                    is_workflow_task, claimed_at, retry_count, max_retries
             FROM horsies_tasks WHERE id = $1",
        )
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read the claimed row");
        assert_eq!(
            execution, "ISOLATED",
            "the in-process run is an isolated run"
        );
        claimed_of(ClaimedRow {
            id: task_id,
            task_name,
            queue_name,
            input,
            task_options,
            execution: crate::core::task::Execution::Isolated,
            is_workflow_task,
            claimed_at,
            retry_count,
            max_retries,
            trace: crate::events::RunTrace::NoCarrier,
        })
    }

    /// The end that the run of the claimed row `task_id` reports, run as an
    /// isolated child runs it (in this process) under the orphan rule
    /// `self_heal`: the start write, the refusal of the stored options or the
    /// body, the result envelope, the finalizing marker. The parent's route
    /// decides the refusal.
    async fn run_in_process(
        broker: &Arc<PostgresBroker>,
        task_fn: RegisteredTask,
        claimed: &Claimed,
        self_heal: OrphanSelfHeal,
    ) -> crate::worker::executor::protocol::RunEnd {
        let task = &claimed.task;
        let mut registry = crate::core::registry::task::TaskRegistry::new();
        registry
            .register(task.task_name.clone(), task_fn)
            .expect("one task registers");
        let work = match crate::worker::executor::slots::dispatch_route(
            task.execution,
            crate::worker::executor::slots::ClaimPlan::for_children(1, 0, 0),
            &task.options,
        ) {
            Ok(crate::worker::executor::slots::DispatchRoute::Isolated { work, .. }) => work,
            Ok(_) | Err(_) => panic!("an isolated row on a plan of one isolated child runs there"),
        };
        let frame = crate::worker::executor::protocol::RunFrame {
            task_id: task.task_id,
            task_name: task.task_name.clone(),
            worker_id: "worker-1".to_owned(),
            claimed_at: task.claimed_at,
            orphan_self_heal: self_heal,
            work,
            trace: task.trace.clone(),
            input: claimed.input.clone(),
        };
        crate::worker::executor::child_host::in_process::run_isolated_in_process(
            Arc::clone(broker),
            Arc::new(registry),
            frame,
        )
        .await
    }

    /// Run the claimed row `task_id` as the worker does: the isolated run
    /// ([`run_in_process`]) under the orphan rule `self_heal`, then the
    /// outcome writer with the reported end, phase 2 included. Returns the
    /// end and the settlement.
    async fn run_finalize_with(
        broker: &Arc<PostgresBroker>,
        task_fn: RegisteredTask,
        task_id: Uuid,
        self_heal: OrphanSelfHeal,
    ) -> (
        crate::worker::executor::protocol::RunEnd,
        crate::worker::executor::slots::Settlement,
    ) {
        let claimed = claimed_from_database(&test_pool().await, task_id).await;
        let end = run_in_process(broker, task_fn, &claimed, self_heal).await;
        let writer = OutcomeWriter::new(
            Arc::clone(broker),
            crate::test_support::child_specs::no_child_specs(),
            "worker-1".to_owned(),
            crate::core::config::payload::PayloadPolicy::default(),
            crate::worker::executor::slots::test_successes(broker, "worker-1"),
        );
        let settled = writer
            .settle(&claimed.task, AttemptEnd::Reported(end.clone()))
            .await;
        (end, settled)
    }

    /// [`run_finalize_with`] under the default recovery config's orphan
    /// rule.
    async fn run_finalize(broker: &Arc<PostgresBroker>, task_fn: RegisteredTask, task_id: Uuid) {
        let (_end, _settled) = run_finalize_with(
            broker,
            task_fn,
            task_id,
            OrphanSelfHeal::of_recovery_flag(
                RecoveryConfig::default().auto_terminate_orphaned_workflow_tasks,
            ),
        )
        .await;
    }

    /// Phase 1 of the success of the workflow task `task_id`, as the outcome
    /// writer writes it, with no phase 2: the in-process run, then the
    /// success write (`persist_ok_result`) of the value the envelope reports.
    /// Returns the phase 2 that the write owes.
    async fn run_phase_one(
        broker: &Arc<PostgresBroker>,
        task_fn: RegisteredTask,
        task_id: Uuid,
    ) -> Phase2Work {
        let claimed = claimed_from_database(&test_pool().await, task_id).await;
        let value = match run_in_process(broker, task_fn, &claimed, OrphanSelfHeal::Off).await {
            crate::worker::executor::protocol::RunEnd::Result(envelope) => {
                let envelope: serde_json::Value =
                    serde_json::from_str(envelope.as_str()).expect("the envelope is JSON");
                serde_json::to_vec(&envelope["ok"]).expect("the value encodes")
            }
            other => panic!("expected the result of the run, got {other:?}"),
        };
        let task = claimed.task;
        let fence = crate::core::lifecycle::OwnedClaim {
            worker_id: "worker-1".to_owned(),
            claimed_at: task.claimed_at,
        };
        let is_workflow_task = task.kind.is_workflow_task();
        match persist_ok_result(broker, task_id, &value, fence, is_workflow_task).await {
            Ok(FinalizeOutcome::Terminal {
                capacity_notified,
                phase_two,
            }) => Phase2Work {
                task_id,
                queue_name: task.queue_name,
                is_workflow_task,
                capacity_notified,
                phase_two,
            },
            Ok(_) => panic!("the success of a held row is terminal"),
            Err(error) => panic!("the success write failed: {error}"),
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

        run_finalize(&broker, test_task(succeed), task_id).await;

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

    /// Insert a RUNNING task owned by a specific worker at a claim generation
    /// of now, simulating a task that was reaper-requeued and re-claimed (then
    /// set RUNNING) by another worker.
    async fn insert_running_task_owned_by(pool: &TestPool, task_id: &Uuid, owner: &str) {
        sql::query(
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
                    'RUNNING', NOW(), NOW(), $2, NULL,
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

        use crate::broker::attempt_writes::{AttemptFailure, AttemptFailureKind};
        use crate::core::lifecycle::TerminalizationOutcome;
        use crate::test_support::terminal_writes::{self, TerminalFailure};

        // A task failure carries an error code; a worker failure carries a
        // failed reason and records a WORKER_FAILURE attempt.
        let task_failure = TerminalFailure::of_task("{}", Some("X"), None);
        let worker_failure = TerminalFailure {
            result_json: "{}".to_owned(),
            error_code: None,
            failed_reason: Some("crash".to_owned()),
            attempt: AttemptFailure {
                kind: AttemptFailureKind::WorkerFailure,
                error_message: None,
                failed_reason: Some("crash".to_owned()),
            },
        };
        let applied = |outcomes: &[TerminalizationOutcome]| {
            matches!(outcomes, [TerminalizationOutcome::Applied { .. }])
        };

        // complete: stale owner rejected, current owner applies.
        let t_complete = Uuid::new_v4();
        insert_running_task_owned_by(&pool, &t_complete, "worker-2").await;
        assert!(
            !applied(
                &terminal_writes::complete_plain_task(&pool, t_complete, "worker-1", "{}")
                    .await
                    .unwrap()
            ),
            "stale owner must not complete the re-claimed task"
        );
        assert_eq!(fetch_task_state(&pool, &t_complete).await.0, "RUNNING");
        assert!(
            applied(
                &terminal_writes::complete_plain_task(&pool, t_complete, "worker-2", "{}")
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
                &terminal_writes::fail_running_task(&pool, t_fail, "worker-1", &task_failure)
                    .await
                    .unwrap()
            ),
            "stale owner must not fail the re-claimed task"
        );
        assert_eq!(fetch_task_state(&pool, &t_fail).await.0, "RUNNING");
        assert!(applied(
            &terminal_writes::fail_running_task(&pool, t_fail, "worker-2", &task_failure)
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
        // current owner applies. Same function as the task failure, with a
        // WORKER_FAILURE attempt.
        let t_worker = Uuid::new_v4();
        insert_running_task_owned_by(&pool, &t_worker, "worker-2").await;
        assert!(
            !applied(
                &terminal_writes::fail_running_task(&pool, t_worker, "worker-1", &worker_failure)
                    .await
                    .unwrap()
            ),
            "stale owner must not worker-fail the re-claimed task"
        );
        assert_eq!(fetch_task_state(&pool, &t_worker).await.0, "RUNNING");
        assert!(applied(
            &terminal_writes::fail_running_task(&pool, t_worker, "worker-2", &worker_failure)
                .await
                .unwrap()
        ));
        assert_eq!(fetch_task_state(&pool, &t_worker).await.0, "FAILED");
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

        run_finalize(&broker, test_task(callback_success_task), task_id).await;

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

    /// A node at index 1 of `workflow_id` that depends on node 0.
    async fn add_dependent_node(pool: &TestPool, workflow_id: Uuid) {
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                queue_name, priority, dependencies, allow_failed_deps, join_type, status,
                is_subworkflow, created_at, max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 1, 'node_1', 'finalize_dependent', '{}', 'default', 100, '{0}',
                FALSE, 'all', 'PENDING', FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// Makes `child_id` the child of a new RUNNING parent workflow, linked
    /// from the parent's sub-workflow node at index 0. Returns the parent id.
    async fn adopt_as_child_workflow(pool: &TestPool, child_id: Uuid) -> Uuid {
        let parent_id = Uuid::new_v4();
        sql::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES (
                $1, 'finalize_parent', 'RUNNING', 'fail', NULL,
                'test.finalize.parent.v1', 0, $1,
                NOW(), NOW(), NOW(), NOW()
            )",
        )
        .bind(parent_id)
        .execute(pool)
        .await
        .unwrap();
        sql::query(
            "UPDATE horsies_workflows
             SET parent_workflow_id = $1, parent_task_index = 0, depth = 1, root_workflow_id = $1
             WHERE id = $2",
        )
        .bind(parent_id)
        .bind(child_id)
        .execute(pool)
        .await
        .unwrap();
        sql::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                queue_name, priority, dependencies, allow_failed_deps, join_type, status,
                is_subworkflow, sub_workflow_id, sub_workflow_name, sub_definition_key,
                sub_workflow_params, created_at, started_at
            ) VALUES (
                'NONE', $1, $2, 0, 'child_node', '__sub_workflow:finalize_wf', '{}', 'default',
                100, '{}', FALSE, 'all', 'RUNNING', TRUE, $3, 'finalize_wf',
                'test.finalize.v1', 'NOT_GIVEN', NOW(), NOW()
            )",
        )
        .bind(Uuid::new_v4())
        .bind(parent_id)
        .bind(child_id)
        .execute(pool)
        .await
        .unwrap();
        parent_id
    }

    /// Phase 1 of a node with a dependent and no sub-workflow node below it
    /// names the one-statement form, and phase 2 promotes the dependent: the
    /// dependent node is ENQUEUED with a minted PENDING task row.
    #[tokio::test]
    #[serial]
    async fn phase_two_of_a_node_with_dependents_promotes_them_in_one_statement() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;
        let task_id = Uuid::new_v4();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        let workflow_id = link_task_to_workflow(&pool, &task_id, None).await;
        add_dependent_node(&pool, workflow_id).await;

        let phase2_work = run_phase_one(&broker, test_task(callback_success_task), task_id).await;
        assert_eq!(
            phase2_work.phase_two,
            crate::worker::execution::PhaseTwoForm::Step(
                crate::core::lifecycle::PhaseTwoStep::DependentsWithoutSubworkflow
            )
        );

        run_phase2(
            &broker,
            &crate::test_support::child_specs::TestChildSpecs::empty(),
            phase2_work,
        )
        .await;

        let (dependent_status, dependent_task): (String, Option<Uuid>) = sql::query_as(
            "SELECT status, task_id FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 1",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(dependent_status, "ENQUEUED");
        let minted_status: String =
            sql::query_scalar("SELECT status FROM horsies_tasks WHERE id = $1")
                .bind(dependent_task.expect("the promoted node links its task row"))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(minted_status, "PENDING");
        let (workflow_status, _) = fetch_workflow_state(&pool, &workflow_id).await;
        assert_eq!(workflow_status, "RUNNING");
    }

    /// The phase 2 that ends a child workflow advances the parent node after
    /// its commit (`horsies_workflow_child_finish`): the sub-workflow node is
    /// COMPLETED and the parent workflow completes.
    #[tokio::test]
    #[serial]
    async fn phase_two_that_ends_a_child_workflow_advances_the_parent_node() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;
        let task_id = Uuid::new_v4();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        let child_id = link_task_to_workflow(&pool, &task_id, None).await;
        let parent_id = adopt_as_child_workflow(&pool, child_id).await;

        let phase2_work = run_phase_one(&broker, test_task(callback_success_task), task_id).await;
        run_phase2(
            &broker,
            &crate::test_support::child_specs::TestChildSpecs::empty(),
            phase2_work,
        )
        .await;

        let (child_status, _) = fetch_workflow_state(&pool, &child_id).await;
        assert_eq!(child_status, "COMPLETED");
        let parent_node_status: String = sql::query_scalar(
            "SELECT status FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = 0",
        )
        .bind(parent_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(parent_node_status, "COMPLETED");
        let (parent_status, _) = fetch_workflow_state(&pool, &parent_id).await;
        assert_eq!(parent_status, "COMPLETED");
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

        let phase2_work = run_phase_one(&broker, test_task(callback_success_task), task_id).await;

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

        let child_specs = crate::test_support::child_specs::TestChildSpecs::empty();
        run_phase2(&broker, &child_specs, phase2_work).await;

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

        let phase2_work = run_phase_one(&broker, test_task(callback_success_task), task_id).await;

        // A broker on a database that does not exist: every checkout fails
        // with a server refusal (3D000), which is not retryable.
        let missing_database = crate::test_support::with_database(
            pool.url(),
            &format!("horsies_missing_{}", Uuid::new_v4().simple()),
        );
        let failed_phase2_broker =
            PostgresBroker::from_test_url(&missing_database, crate::ClientFamily::Rust);

        let child_specs = crate::test_support::child_specs::TestChildSpecs::empty();
        run_phase2(&failed_phase2_broker, &child_specs, phase2_work).await;

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

    /// Python's start of a workflow task: a database error in the start
    /// gives no run (`core/worker/child_runner.py`, OWNERSHIP_UNCONFIRMED),
    /// and the claim is released at once with the fenced release. Before, a
    /// failed node statement was treated as informational and user code ran.
    #[tokio::test]
    #[serial]
    async fn a_failed_workflow_node_check_runs_no_user_code_and_releases_the_claim() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        link_task_to_workflow(&pool, &task_id, Some(0)).await;
        EXECUTION_COUNT.store(0, Ordering::SeqCst);

        let (function_name, trigger_name) = install_fail_workflow_task_running_trigger(&pool).await;
        let result = AssertUnwindSafe(async {
            run_finalize(&broker, test_task(counted_success_task), task_id).await;

            assert_eq!(
                EXECUTION_COUNT.load(Ordering::SeqCst),
                0,
                "user code must not run when the workflow node check fails",
            );
            let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
            assert_eq!(status, "PENDING", "the claim is released at once");
            assert_eq!(result, None);
            assert_eq!(error_code, None);
            assert!(broker.get_task_attempts(task_id).await.unwrap().is_empty());
        })
        .catch_unwind()
        .await;

        remove_test_trigger(&pool, &function_name, &trigger_name).await;

        if let Err(panic) = result {
            resume_unwind(panic);
        }
    }

    /// Python `test_orphaned_workflow_task_repro.py::test_workflow_check_failed_disabled_leaves_orphan_claimed`:
    /// a workflow task with no runnable node does not run; with orphan
    /// self-heal off nothing is written and the row stays CLAIMED.
    #[tokio::test]
    #[serial]
    async fn an_unlinked_workflow_task_with_self_heal_off_runs_nothing_and_stays_claimed() {
        let pool = test_pool().await;
        let broker = test_broker().await;
        clean(&pool).await;

        let task_id = Uuid::new_v4();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;
        // A workflow task carries no command fingerprint
        // (`horsies_tasks_command_fingerprint_by_origin`).
        sql::query(
            "UPDATE horsies_tasks
             SET is_workflow_task = TRUE,
                 command_fingerprint = NULL,
                 command_fingerprint_version = NULL
             WHERE id = $1",
        )
        .bind(task_id)
        .execute(&pool)
        .await
        .unwrap();
        EXECUTION_COUNT.store(0, Ordering::SeqCst);

        let (end, settled) = run_finalize_with(
            &broker,
            test_task(counted_success_task),
            task_id,
            OrphanSelfHeal::Off,
        )
        .await;

        assert!(matches!(
            end,
            crate::worker::executor::protocol::RunEnd::StoppedBeforeRunning(
                crate::worker::executor::protocol::StopReason::NodeNotLinked
            )
        ));
        assert_eq!(
            settled,
            crate::worker::executor::slots::Settlement::NothingOwed
        );
        assert_eq!(EXECUTION_COUNT.load(Ordering::SeqCst), 0);
        let (status, _, _) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "CLAIMED");
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
        sql::query("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = $1")
            .bind(&workflow_id)
            .execute(&pool)
            .await
            .unwrap();
        EXECUTION_COUNT.store(0, Ordering::SeqCst);

        run_finalize(&broker, test_task(counted_success_task), task_id).await;

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
        sql::query("UPDATE horsies_workflows SET status = 'CANCELLED' WHERE id = $1")
            .bind(&workflow_id)
            .execute(&pool)
            .await
            .unwrap();
        EXECUTION_COUNT.store(0, Ordering::SeqCst);

        run_finalize(&broker, test_task(counted_success_task), task_id).await;

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

    /// Stored options that do not parse fail the task before user code runs,
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

        run_finalize(&broker, test_task(succeed), task_id).await;

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

    /// A stored timeout above the limit refuses the run, and a readable
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

        run_finalize(&broker, test_task(succeed), task_id).await;

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

        run_finalize(&broker, test_task(retryable_failure), task_id).await;

        let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "PENDING");
        assert_eq!(result, None);
        assert_eq!(error_code, None);

        let retry_count: i32 =
            sql::query_scalar("SELECT retry_count FROM horsies_tasks WHERE id = $1")
                .bind(&task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(retry_count, 1);

        let next_retry_at: Option<chrono::DateTime<Utc>> =
            sql::query_scalar("SELECT next_retry_at FROM horsies_tasks WHERE id = $1")
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

        run_finalize(&broker, test_task(fatal_failure), task_id).await;

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
        let task_error_code = OperationalErrorCode::UnhandledException.to_string();

        let task_id = Uuid::new_v4();
        insert_claimed_task(&pool, &task_id, "default", 0, 0, None).await;

        run_finalize(&broker, test_task(panic_task), task_id).await;

        let (status, result, error_code) = fetch_task_state(&pool, &task_id).await;
        assert_eq!(status, "FAILED");
        assert_eq!(error_code.as_deref(), Some(task_error_code.as_str()));

        let persisted: TaskResult<serde_json::Value> =
            serde_json::from_str(result.as_deref().unwrap()).unwrap();
        let persisted_error = persisted.unwrap_err();
        // #1402: the panic record of Python's unhandled exception, with
        // `panic` as the exception type and the module of the task.
        let message = persisted_error.message.clone().unwrap_or_default();
        assert!(
            message.starts_with("Unhandled exception in task ")
                && message.ends_with(": panic: task panicked before finalize"),
            "{message}"
        );
        assert_eq!(
            persisted_error.data_value(),
            Some(serde_json::json!({ "exception_type": "panic" }))
        );
        let record = persisted_error
            .exception
            .expect("a panic stores an exception record");
        assert_eq!(record.type_name, "panic");
        assert_eq!(record.module, module_path!());
        assert_eq!(record.message, "task panicked before finalize");
        assert_eq!(record.repr, "task panicked before finalize");
    }

    /// A database error during the failed-attempt write decides neither the
    /// retry nor the failure: phase 1 reports a phase-1 error and writes
    /// nothing, and the next try sends the same statement. Before, an error
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

        let unreachable = unreachable_broker();

        let Err(err) = persist_terminal_state(
            &unreachable,
            &claimed_task(
                &task_id,
                "default",
                Some(task_options.as_str()),
                RetryFacts {
                    retry_count: 0,
                    max_retries: 3,
                },
            ),
            TaskResult::Err(TaskError::new("RETRY_ME", "try again").unwrap()),
            "worker-1",
            &crate::core::config::payload::PayloadPolicy::default(),
        )
        .await
        else {
            panic!("an unreachable database must fail the failed-attempt write");
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

        let unreachable = unreachable_broker();

        let Err(err) = persist_terminal_state(
            &unreachable,
            &claimed_task(
                &task_id,
                "default",
                None,
                RetryFacts {
                    retry_count: 0,
                    max_retries: 0,
                },
            ),
            TaskResult::Ok(br#""phase1-durable""#.to_vec()),
            "worker-1",
            &crate::core::config::payload::PayloadPolicy::default(),
        )
        .await
        else {
            panic!("an unreachable database must fail the phase-1 write");
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
            task_id,
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
    /// listen on task_new (parity with horsies PR #101 cafc9200). The payload
    /// is the constant `capacity` that the baseline functions also send.
    #[tokio::test]
    #[serial]
    async fn capacity_wake_emits_queue_specific_signal_only() {
        let pool = test_pool().await;
        clean(&pool).await;

        let mut listener = TestListener::connect_with(&pool).await.unwrap();
        listener.listen("task_new").await.unwrap();
        listener.listen("task_queue_high-priority").await.unwrap();

        let core_pool = crate::broker::pools::core_pool_for_test_url(pool.url());
        send_phase2_wake(
            &core_pool,
            crate::broker::core_rows::StatementLog::Off,
            &capacity_wake("high-priority"),
        )
        .await
        .expect("send the capacity wake");

        let first = tokio::time::timeout(Duration::from_secs(2), listener.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (first.channel().to_owned(), first.payload().to_owned()),
            ("task_queue_high-priority".to_owned(), "capacity".to_owned()),
        );

        // No second signal: task_new must not be emitted.
        let second = tokio::time::timeout(Duration::from_millis(500), listener.recv()).await;
        assert!(
            second.is_err(),
            "capacity signal must not be emitted on task_new"
        );
    }

    /// A capacity wake that the database refuses is an error, never dropped
    /// (#722): a closed pool answers `PoolClosed`, and nothing was sent.
    #[tokio::test]
    #[serial]
    async fn capacity_wake_on_a_closed_pool_is_an_error() {
        let pool = test_pool().await;
        let core_pool = crate::broker::pools::core_pool_for_test_url(pool.url());
        core_pool.close();
        let sent = send_phase2_wake(
            &core_pool,
            crate::broker::core_rows::StatementLog::Off,
            &capacity_wake("default"),
        )
        .await;
        assert!(
            matches!(
                sent,
                Err(crate::db::StatementFailure::NotSent {
                    cause: crate::db::NotSentCause::Checkout(
                        crate::db::CheckoutFailure::PoolClosed { .. }
                    ),
                    ..
                })
            ),
            "{sent:?}"
        );
    }

    /// A database of its own, migrated without the install's family record,
    /// for a worker start. Dropped with `FreshDatabase::drop`.
    struct FreshDatabase {
        pool: TestPool,
        database: crate::test_support::TestDatabase,
        /// The shared test database URL, for the worker's config only: the
        /// worker reaches this database through `pool`.
        config_url: String,
    }

    impl FreshDatabase {
        /// A copy of the installed test template (`crate::test_support`)
        /// with its family row removed.
        async fn create() -> Self {
            let config_url = crate::test_support::migrated_database_url().await;
            let database = crate::test_support::TestDatabase::create().await;
            let pool = TestPoolOptions::new()
                .max_connections(8)
                .connect(database.url())
                .await
                .expect("connect start test database");
            // The template holds this client's family row; a start test
            // records the family it needs itself.
            sql::query("DELETE FROM horsies_client_family")
                .execute(&pool)
                .await
                .expect("remove the family row of the install");
            Self {
                pool,
                database,
                config_url,
            }
        }

        async fn record_family(&self, family: &str) {
            sql::query(
                "INSERT INTO horsies_client_family (only_row, family, recorded_at)
                 VALUES (TRUE, $1, NOW())",
            )
            .bind(family)
            .execute(&self.pool)
            .await
            .expect("record a client family");
        }

        async fn drop(self) {
            self.pool.close().await;
            self.database.drop().await;
        }
    }

    /// A worker on `broker` that retries a failed start at most once.
    fn start_test_worker(broker: PostgresBroker, config_url: String) -> Worker {
        worker_with_config(broker, test_app_config(config_url))
    }

    fn worker_with_config(broker: PostgresBroker, app_config: AppConfig) -> Worker {
        worker_of_one_idle_child(Arc::new(broker), app_config, WorkerConfig::default())
    }

    /// The app config of the start tests: no `claim_lease_ms`, one start
    /// attempt.
    fn test_app_config(config_url: String) -> AppConfig {
        use crate::core::config::{PostgresConfig, QueueMode, WorkerResilienceConfig};

        AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: config_url,
                session_database_url: None,
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                worker_pool_pre_ping: false,
                pool_size: 30,
                max_overflow: 30,
                retain_rerun_input_default: false,
                pool_timeout: 30,
                pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
                echo: false,
                tcp_keepalives: Default::default(),
                tcp_user_timeout: Default::default(),
            },
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: crate::core::RetentionConfig::default(),
            resilience: WorkerResilienceConfig {
                db_retry_initial_ms: 100,
                db_retry_max_ms: 100,
                db_retry_max_attempts: 1,
                notify_poll_interval_ms: 100,
            },
            worker_profiles: std::collections::BTreeMap::new(),
            schedule: None,
            resend_on_transient_err: false,
        }
    }

    /// One PENDING task on the default queue; a claim loop would take it.
    async fn insert_pending_task(pool: &TestPool) -> Uuid {
        let task_id = Uuid::new_v4();
        sql::query(
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
                 $1, 'start_test', 'default', 100, '{}', NULL, 'ISOLATED',
                 'forever', NOW(), NOW(),
                 1, decode(repeat('00', 32), 'hex'), NULL,
                 'PENDING', NOW(), NULL, NULL, NULL,
                 NULL, NULL, NULL,
                 NULL, NULL, NULL,
                 0, 0, NULL, NULL, FALSE,
                 NOW(), FALSE, NULL, NULL
             )",
        )
        .bind(task_id)
        .execute(pool)
        .await
        .expect("insert a pending task");
        task_id
    }

    /// What a started worker writes: catalog leaves and retention classes
    /// (startup coverage), worker states (state loop), heartbeats (beat
    /// writers), and the claim of the PENDING task (claim loop).
    #[derive(Debug, PartialEq, Eq)]
    struct StartFacts {
        catalog_leaves: i64,
        retention_classes: i64,
        worker_states: i64,
        heartbeats: i64,
        pending_status: String,
        pending_claimed_by: Option<String>,
    }

    async fn start_facts(pool: &TestPool, pending: Uuid) -> StartFacts {
        use crate::core::history::names::{LEAF_CATALOG, RETENTION_CLASSES};

        let (catalog_leaves, retention_classes, worker_states, heartbeats): (i64, i64, i64, i64) =
            sql::query_as(&format!(
                "SELECT (SELECT count(*) FROM {LEAF_CATALOG}),
                        (SELECT count(*) FROM {RETENTION_CLASSES}),
                        (SELECT count(*) FROM horsies_worker_states),
                        (SELECT count(*) FROM horsies_heartbeats)"
            ))
            .fetch_one(pool)
            .await
            .expect("read start facts");
        let (pending_status, pending_claimed_by): (String, Option<String>) =
            sql::query_as("SELECT status, claimed_by_worker_id FROM horsies_tasks WHERE id = $1")
                .bind(pending)
                .fetch_one(pool)
                .await
                .expect("read the pending task");
        StartFacts {
            catalog_leaves,
            retention_classes,
            worker_states,
            heartbeats,
            pending_status,
            pending_claimed_by,
        }
    }

    /// Run the worker; its start must fail and `run` must return.
    async fn refused_run(worker: &Worker) -> WorkerError {
        tokio::time::timeout(Duration::from_secs(30), worker.run())
            .await
            .expect("a refused start returns from run")
            .expect_err("the start must be refused")
    }

    /// With the install on, a database of the Python family fails the schema
    /// step: no coverage, no runtime loop, no claim.
    #[tokio::test]
    #[serial]
    async fn a_start_refused_at_the_schema_step_writes_nothing_later() {
        use crate::broker::client_family::{ClientFamily, RecordedFamily};
        use crate::broker::BrokerError;

        let database = FreshDatabase::create().await;
        database.record_family("PYTHON").await;
        let pending = insert_pending_task(&database.pool).await;
        let before = start_facts(&database.pool, pending).await;
        let worker = start_test_worker(
            PostgresBroker::from_test_url(database.pool.url(), crate::ClientFamily::Rust),
            database.config_url.clone(),
        );

        let error = refused_run(&worker).await;

        assert!(matches!(
            error,
            WorkerError::Broker(BrokerError::ClientFamilyMismatch {
                stored: RecordedFamily::Recorded(ClientFamily::Python),
                own: ClientFamily::Rust,
            })
        ));
        assert_eq!(start_facts(&database.pool, pending).await, before);
        drop(worker);
        database.drop().await;
    }

    /// With the install off, a database with no family row and one of the
    /// Python family fail the family step: no coverage, no runtime loop, no
    /// claim. Both refusals are not retryable.
    #[tokio::test]
    #[serial]
    async fn a_start_refused_at_the_family_step_writes_nothing_later() {
        use crate::broker::client_family::{ClientFamily, RecordedFamily};
        use crate::broker::BrokerError;

        let database = FreshDatabase::create().await;
        let pending = insert_pending_task(&database.pool).await;
        let before = start_facts(&database.pool, pending).await;

        let worker = start_test_worker(
            PostgresBroker::from_test_url_observe_only(
                database.pool.url(),
                crate::ClientFamily::Rust,
            ),
            database.config_url.clone(),
        );
        let not_recorded = refused_run(&worker).await;
        assert!(matches!(
            not_recorded,
            WorkerError::Broker(BrokerError::ClientFamilyMismatch {
                stored: RecordedFamily::NotRecorded,
                own: ClientFamily::Rust,
            })
        ));
        assert!(!not_recorded.is_retryable());
        assert_eq!(start_facts(&database.pool, pending).await, before);
        drop(worker);

        database.record_family("PYTHON").await;
        let worker = start_test_worker(
            PostgresBroker::from_test_url_observe_only(
                database.pool.url(),
                crate::ClientFamily::Rust,
            ),
            database.config_url.clone(),
        );
        let python = refused_run(&worker).await;
        assert!(matches!(
            python,
            WorkerError::Broker(BrokerError::ClientFamilyMismatch {
                stored: RecordedFamily::Recorded(ClientFamily::Python),
                own: ClientFamily::Rust,
            })
        ));
        assert!(!python.is_retryable());
        assert_eq!(start_facts(&database.pool, pending).await, before);
        drop(worker);
        database.drop().await;
    }

    /// The first words of the info line of a start that waits for the
    /// coverage gate ([`StartRetryCause::log_retry`]).
    const GATE_WAIT_LINE: &str = "Worker start waits for the partition coverage gate";

    /// Takes the coverage gate in the transaction of `holder`, as the gated
    /// pass of another process does. The gate is held until the transaction
    /// ends.
    async fn hold_coverage_gate(holder: &mut sql::TestTransaction<'_>) {
        let held: bool = sql::query_scalar(
            "SELECT pg_try_advisory_xact_lock(
                 hashtextextended('horsies:partition-coverage:v1', 1601)
             )",
        )
        .fetch_one(&mut **holder)
        .await
        .expect("hold the coverage gate");
        assert!(held, "the coverage gate is free before the test holds it");
    }

    /// Takes the creation locks of the heartbeat leaves of the current UTC
    /// hour and of the next one in the transaction of `holder`, so a leaf
    /// creation for either hour answers `Busy` while the transaction lasts.
    /// The next hour keeps the test valid when the hour turns during it.
    async fn hold_current_heartbeat_leaf_locks(holder: &mut sql::TestTransaction<'_>) {
        use crate::core::history::names::{HEARTBEAT_CLASS_KEY, LEAF_LOCK_KEY_FUNCTION};
        use chrono::{Duration as ChronoDuration, Timelike};

        let now: chrono::DateTime<Utc> = sql::query_scalar("SELECT statement_timestamp()")
            .fetch_one(&mut **holder)
            .await
            .expect("read the database time");
        let hour = now
            .with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .expect("truncate to the hour");
        for lower in [hour, hour + ChronoDuration::hours(1)] {
            sql::query(&format!(
                "SELECT pg_advisory_xact_lock({LEAF_LOCK_KEY_FUNCTION}($1, $2))"
            ))
            .bind(HEARTBEAT_CLASS_KEY)
            .bind(lower)
            .execute(&mut **holder)
            .await
            .expect("hold the creation lock of a current heartbeat leaf");
        }
    }

    /// A startup coverage refusal other than the busy coverage gate refuses
    /// the start at once, not retryable: here another session holds the
    /// creation locks of the current heartbeat leaves, so the leaf creation
    /// answers `Busy` and no heartbeat leaf covers the present instant. No
    /// runtime loop starts: no worker state, no heartbeat, no claim.
    #[tokio::test]
    #[serial]
    async fn a_start_refused_at_the_coverage_step_starts_no_runtime_loop() {
        let database = FreshDatabase::create().await;
        database.record_family("RUST").await;
        let pending = insert_pending_task(&database.pool).await;
        let mut holder = database.pool.begin().await.expect("begin leaf lock holder");
        hold_current_heartbeat_leaf_locks(&mut holder).await;
        let before = start_facts(&database.pool, pending).await;
        let worker = start_test_worker(
            PostgresBroker::from_test_url(database.pool.url(), crate::ClientFamily::Rust),
            database.config_url.clone(),
        );

        let error = refused_run(&worker).await;

        assert!(
            matches!(&error, WorkerError::Config(detail) if detail.contains("startup coverage refused")),
            "{error:?}"
        );
        assert!(!error.is_retryable());
        let after = start_facts(&database.pool, pending).await;
        assert_eq!(after.worker_states, before.worker_states);
        assert_eq!(after.heartbeats, before.heartbeats);
        assert_eq!(after.pending_status, "PENDING");
        assert_eq!(after.pending_claimed_by, None);
        holder.rollback().await.expect("release the leaf locks");
        drop(worker);
        database.drop().await;
    }

    /// A start that finds the coverage gate held by another session, with
    /// no current heartbeat leaf, waits with the resilience backoff and logs
    /// one info line that names the cause. The other session then releases
    /// the gate; the next attempt takes it, creates the current heartbeat
    /// leaf, and the start ends `Started` with no error line.
    #[tokio::test]
    #[serial]
    async fn a_start_that_finds_the_coverage_gate_held_starts_once_the_gate_is_released() {
        use crate::core::config::WorkerResilienceConfig;
        use crate::core::history::names::{HEARTBEAT_CLASS_KEY, LEAF_CATALOG};

        let database = FreshDatabase::create().await;
        database.record_family("RUST").await;
        let mut holder = database.pool.begin().await.expect("begin gate holder");
        hold_coverage_gate(&mut holder).await;
        let worker = worker_with_config(
            PostgresBroker::from_test_url(database.pool.url(), crate::ClientFamily::Rust),
            AppConfig {
                // A wait of at least 0.75 s gives the release time to land
                // before the second attempt.
                resilience: WorkerResilienceConfig {
                    db_retry_initial_ms: 1_000,
                    db_retry_max_ms: 1_000,
                    db_retry_max_attempts: 3,
                    notify_poll_interval_ms: 100,
                },
                ..test_app_config(database.config_url.clone())
            },
        );
        let capture = crate::test_support::LogCapture::new();
        let guard = capture.set_default();
        let release_after_the_first_wait = async {
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            while capture
                .lines_starting(tracing::Level::INFO, GATE_WAIT_LINE)
                .is_empty()
            {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the start did not wait for the gate"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            holder.rollback().await.expect("release the coverage gate");
        };

        let (started, ()) = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(
                worker.start_database_with_resilience(),
                release_after_the_first_wait
            )
        })
        .await
        .expect("the start ends within the bound");
        drop(guard);

        assert_eq!(
            started.expect("the start ends without an error"),
            StartOutcome::Started
        );
        let waits = capture.lines_starting(tracing::Level::INFO, GATE_WAIT_LINE);
        assert_eq!(waits.len(), 1, "{waits:?}");
        assert!(
            waits[0]
                .message
                .contains("another process creates the history partitions"),
            "{waits:?}"
        );
        assert!(
            capture
                .lines_starting(tracing::Level::ERROR, "Worker start failed")
                .is_empty(),
            "{:?}",
            capture.events()
        );
        let current_heartbeat_leaves: i64 = sql::query_scalar(&format!(
            "SELECT count(*) FROM {LEAF_CATALOG}
             WHERE class_key = $1
               AND lower_anchor <= statement_timestamp()
               AND upper_anchor > statement_timestamp()"
        ))
        .bind(HEARTBEAT_CLASS_KEY)
        .fetch_one(&database.pool)
        .await
        .expect("read the current heartbeat leaf");
        assert_eq!(current_heartbeat_leaves, 1);
        drop(worker);
        database.drop().await;
    }

    /// A start whose coverage gate another session holds through the whole
    /// retry budget, with no current heartbeat leaf, is refused with
    /// `WorkerError::CoverageGateBusy`: the error names the busy gate, the
    /// time waited and the attempts. Each retry wait logs one info line. No
    /// coverage, no runtime loop, no claim.
    #[tokio::test]
    #[serial]
    async fn a_start_whose_coverage_gate_stays_held_past_the_budget_is_refused() {
        let database = FreshDatabase::create().await;
        database.record_family("RUST").await;
        let pending = insert_pending_task(&database.pool).await;
        let mut holder = database.pool.begin().await.expect("begin gate holder");
        hold_coverage_gate(&mut holder).await;
        let before = start_facts(&database.pool, pending).await;
        let worker = start_test_worker(
            PostgresBroker::from_test_url(database.pool.url(), crate::ClientFamily::Rust),
            database.config_url.clone(),
        );
        let capture = crate::test_support::LogCapture::new();
        let guard = capture.set_default();
        let run_started = std::time::Instant::now();

        let error = refused_run(&worker).await;
        let run_took = run_started.elapsed();
        drop(guard);

        let WorkerError::CoverageGateBusy { waited, attempts } = &error else {
            panic!("a gate held past the budget is CoverageGateBusy: {error:?}");
        };
        // `test_app_config`: one retry, after a wait of at least 0.1 s.
        assert_eq!(*attempts, 2);
        assert!(
            *waited >= Duration::from_millis(100) && *waited <= run_took,
            "{waited:?} of {run_took:?}"
        );
        let message = error.to_string();
        assert!(
            message.contains("partition coverage gate (coverage_gate_busy)")
                && message.contains(&format!("waited {} ms over 2 attempts", waited.as_millis())),
            "{message}"
        );
        assert!(error.is_retryable());
        assert_eq!(
            capture
                .lines_starting(tracing::Level::INFO, GATE_WAIT_LINE)
                .len(),
            1
        );
        assert_eq!(
            capture
                .lines_starting(
                    tracing::Level::ERROR,
                    "Worker start failed after 2 attempts: the partition coverage gate"
                )
                .len(),
            1,
            "{:?}",
            capture.events()
        );
        assert_eq!(start_facts(&database.pool, pending).await, before);
        holder.rollback().await.expect("release the coverage gate");
        drop(worker);
        database.drop().await;
    }

    /// The startup coverage runs on the core pools of a broker with a
    /// separate session URL: the database start completes.
    #[tokio::test]
    #[serial]
    async fn startup_coverage_runs_on_the_session_pool() {
        use crate::core::config::PostgresConfig;

        let main_url = crate::test_support::migrated_database_url().await;
        let separator = if main_url.contains('?') { '&' } else { '?' };
        let session_url = format!("{main_url}{separator}application_name=start_session");
        let broker = PostgresBroker::connect_with(
            &PostgresConfig {
                database_url: main_url.clone(),
                session_database_url: Some(session_url),
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                worker_pool_pre_ping: false,
                pool_size: 30,
                max_overflow: 30,
                retain_rerun_input_default: false,
                pool_timeout: 30,
                pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
                echo: false,
                tcp_keepalives: Default::default(),
                tcp_user_timeout: Default::default(),
            },
            crate::ClientFamily::Rust,
        )
        .await
        .expect("connect broker with a separate session pool");
        let worker = start_test_worker(broker, main_url);

        let started = worker
            .start_database()
            .await
            .expect("the database start uses only the session pool");
        assert_eq!(started, DatabaseStart::Ready);
    }

    /// The URL of database `database` on the server of `url`.
    fn database_url_for(url: &str, database: &str) -> String {
        let (address, query) = match url.split_once('?') {
            Some((address, query)) => (address, Some(query)),
            None => (url, None),
        };
        let (server, _) = address
            .rsplit_once('/')
            .expect("a test database URL names its database after the last slash");
        match query {
            Some(query) => format!("{server}/{database}?{query}"),
            None => format!("{server}/{database}"),
        }
    }

    /// `url` with `application_name` set, so a trigger can tell its sessions.
    fn with_application_name(url: &str, name: &str) -> String {
        let separator = if url.contains('?') { '&' } else { '?' };
        format!("{url}{separator}application_name={name}")
    }

    /// A CLAIMED task of `worker_id` on the default queue, claimed now, whose
    /// lease ends in ten seconds.
    async fn insert_task_claimed_by(pool: &TestPool, worker_id: &str) -> Uuid {
        let task_id = Uuid::new_v4();
        sql::query(
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
                 $1, 'start_test', 'default', 100, '{}', NULL, 'ISOLATED',
                 'forever', NOW(), NOW(),
                 1, decode(repeat('00', 32), 'hex'), NULL,
                 'CLAIMED', NOW(), NOW(), $2, NOW() + interval '10 seconds',
                 NULL, NULL, NULL,
                 NULL, NULL, NULL,
                 0, 0, NULL, NULL, FALSE,
                 NOW(), FALSE, NULL, NULL
             )",
        )
        .bind(task_id)
        .bind(worker_id)
        .execute(pool)
        .await
        .expect("insert a claimed task");
        task_id
    }

    /// With `claim_lease_ms` not set, the claim of a PENDING task and the
    /// claimer's renewal of a held claim both write the default lease. A
    /// trigger logs every lease written on a CLAIMED row.
    #[tokio::test]
    #[serial]
    async fn claims_and_renewals_write_the_default_lease() {
        let database = FreshDatabase::create().await;
        database.record_family("RUST").await;
        sql::raw_sql(
            "CREATE TABLE test_claim_lease_log (task_id uuid NOT NULL, lease interval NOT NULL);
             CREATE FUNCTION test_log_claim_lease() RETURNS trigger LANGUAGE plpgsql AS $body$
             BEGIN
                 IF NEW.status = 'CLAIMED' AND NEW.claim_expires_at IS NOT NULL THEN
                     INSERT INTO test_claim_lease_log
                     VALUES (NEW.id, NEW.claim_expires_at - now());
                 END IF;
                 RETURN NULL;
             END
             $body$;
             CREATE TRIGGER test_log_claim_lease AFTER UPDATE ON horsies_tasks
                 FOR EACH ROW EXECUTE FUNCTION test_log_claim_lease();",
        )
        .execute(&database.pool)
        .await
        .expect("install the lease log");
        // The worker writes its plain successes on the worker pool and
        // claims on the wakes of its listener: a broker of a worker.
        let worker_url = database_url_for(&database.config_url, database.database.name());
        let broker = PostgresBroker::connect_worker_with(
            &test_app_config(worker_url.clone()).broker,
            crate::ClientFamily::Rust,
        )
        .await
        .expect("connect the broker of a worker");
        // Two isolated children: the held claim counts in the isolated
        // budget (`horsies_claim_in_flight`) and fills one slot; the claim
        // of the pending task takes the other.
        let worker = worker_of_idle_children(
            Arc::new(broker),
            test_app_config(worker_url),
            WorkerConfig::default(),
            2,
        );
        let pending = insert_pending_task(&database.pool).await;
        let held = insert_task_claimed_by(&database.pool, worker.worker_id()).await;
        let cancel = worker.cancel_token();
        let run = tokio::spawn(async move { worker.run().await });

        let lease_seconds = |task_id: Uuid| {
            let pool = database.pool.clone();
            async move {
                sql::query_scalar::<f64>(
                    "SELECT EXTRACT(EPOCH FROM lease)::float8 FROM test_claim_lease_log
                     WHERE task_id = $1",
                )
                .bind(task_id)
                .fetch_all(&pool)
                .await
                .expect("read the lease log")
            }
        };
        let (claimed, renewed) = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let claimed = lease_seconds(pending).await;
                let renewed = lease_seconds(held).await;
                if !claimed.is_empty() && !renewed.is_empty() {
                    break (claimed, renewed);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the worker claims the pending task and renews the held claim");
        cancel.cancel();
        let _stopped = tokio::time::timeout(Duration::from_secs(30), run)
            .await
            .expect("the worker stops after cancel")
            .expect("run task join");

        let default_seconds = f64::from(crate::core::defaults::DEFAULT_CLAIM_LEASE_MS) / 1000.0;
        for lease in claimed.iter().chain(renewed.iter()) {
            assert!(
                (default_seconds - 5.0..=default_seconds + 1.0).contains(lease),
                "lease {lease}s is not the default {default_seconds}s"
            );
        }
        database.drop().await;
    }

    /// Python `test_worker_helpers.py::TestRunForever::test_retryable_error_backoff_and_retry`:
    /// a claim pass that fails with an error that can clear is logged, the
    /// loop backs off, and it claims again. The failure comes from the
    /// database: a trigger refuses the first claim of a PENDING row with
    /// SQLSTATE 40001 (`serialization_failure`, class 40: may clear); a
    /// sequence counts the claims, since `nextval` is not rolled back with
    /// the refused statement.
    ///
    /// A log table holds each claim that committed: its row is written by
    /// an AFTER trigger in the transaction of the claim, so the refused
    /// claim writes none. The claim after the backoff is read from the log
    /// while the worker runs. The row's status cannot show it: an idle child
    /// ends at its run frame, and the crash end returns the CLAIMED row to
    /// the queue (`ClaimedRowRule::Release`), at the stop at the latest.
    #[tokio::test]
    #[serial]
    async fn a_claim_error_that_can_clear_backs_off_and_the_loop_claims_again() {
        let database = FreshDatabase::create().await;
        database.record_family("RUST").await;
        sql::raw_sql(
            "CREATE SEQUENCE test_claim_tries;
             CREATE FUNCTION test_refuse_first_claim() RETURNS trigger LANGUAGE plpgsql AS $body$
             BEGIN
                 IF nextval('test_claim_tries') = 1 THEN
                     RAISE EXCEPTION 'a serialization failure for the test'
                         USING ERRCODE = 'serialization_failure';
                 END IF;
                 RETURN NEW;
             END
             $body$;
             CREATE TRIGGER test_refuse_first_claim BEFORE UPDATE ON horsies_tasks
                 FOR EACH ROW WHEN (OLD.status = 'PENDING' AND NEW.status = 'CLAIMED')
                 EXECUTE FUNCTION test_refuse_first_claim();
             CREATE TABLE test_committed_claims (task_id uuid NOT NULL);
             CREATE FUNCTION test_log_committed_claim() RETURNS trigger LANGUAGE plpgsql AS $body$
             BEGIN
                 INSERT INTO test_committed_claims VALUES (NEW.id);
                 RETURN NULL;
             END
             $body$;
             CREATE TRIGGER test_log_committed_claim AFTER UPDATE ON horsies_tasks
                 FOR EACH ROW WHEN (OLD.status = 'PENDING' AND NEW.status = 'CLAIMED')
                 EXECUTE FUNCTION test_log_committed_claim();",
        )
        .execute(&database.pool)
        .await
        .expect("install the claim refusal");
        let worker_url = database_url_for(&database.config_url, database.database.name());
        let broker = PostgresBroker::connect_worker_with(
            &test_app_config(worker_url.clone()).broker,
            crate::ClientFamily::Rust,
        )
        .await
        .expect("connect the broker of a worker");
        let worker = worker_of_idle_children(
            Arc::new(broker),
            test_app_config(worker_url),
            WorkerConfig::default(),
            1,
        );
        let pending = insert_pending_task(&database.pool).await;
        let capture = crate::test_support::LogCapture::new();
        let guard = capture.set_default();
        let cancel = worker.cancel_token();
        let run = tokio::spawn(async move { worker.run().await });

        let claim_tries = || {
            let pool = database.pool.clone();
            async move {
                sql::query_scalar::<i64>(
                    "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM test_claim_tries",
                )
                .fetch_one(&pool)
                .await
                .expect("read the claim count")
            }
        };
        let committed_claims = || {
            let pool = database.pool.clone();
            async move {
                sql::query_scalar::<i64>(
                    "SELECT count(*) FROM test_committed_claims WHERE task_id = $1",
                )
                .bind(pending)
                .fetch_one(&pool)
                .await
                .expect("read the committed claims")
            }
        };
        let claimed = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let claimed = committed_claims().await;
                if claimed >= 1 {
                    break claimed;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the loop claims again after the refused pass");
        let tries = claim_tries().await;
        cancel.cancel();
        let _stopped = tokio::time::timeout(Duration::from_secs(30), run)
            .await
            .expect("the worker stops after cancel")
            .expect("run task join");
        drop(guard);

        let retry_lines: Vec<String> = capture
            .events()
            .into_iter()
            .filter(|event| {
                event.level == tracing::Level::WARN
                    && event.message.starts_with("Worker loop error:")
                    && event.message.contains("Retrying in")
            })
            .map(|event| event.message)
            .collect();
        assert_eq!(retry_lines.len(), 1, "{retry_lines:?}");
        assert!(tries >= 2, "the claim was tried again: {tries}");
        assert!(
            claimed >= 1,
            "the claim after the backoff took the task: {claimed}"
        );
        database.drop().await;
    }

    /// The reaper's recovery statements run on the runtime pool and its
    /// partition maintenance on the session pool. Each pool has its own
    /// `application_name`; statement triggers log which session wrote.
    #[tokio::test]
    #[serial]
    async fn the_reaper_recovers_on_the_runtime_pool_and_maintains_on_the_session_pool() {
        use crate::core::config::PostgresConfig;
        use crate::core::history::names::LEAF_CATALOG;

        let database = FreshDatabase::create().await;
        // The worker start records the family; the coverage pass writes
        // only into a database of the client's own family.
        database.record_family("RUST").await;
        sql::raw_sql(&format!(
            "CREATE TABLE test_pool_writes (relation_name text NOT NULL, application_name text NOT NULL);
             CREATE FUNCTION test_record_pool_write() RETURNS trigger LANGUAGE plpgsql AS $body$
             BEGIN
                 INSERT INTO test_pool_writes
                 VALUES (TG_TABLE_NAME, current_setting('application_name'));
                 RETURN NULL;
             END
             $body$;
             CREATE TRIGGER test_task_writes AFTER UPDATE ON horsies_tasks
                 FOR EACH STATEMENT EXECUTE FUNCTION test_record_pool_write();
             CREATE TRIGGER test_catalog_writes AFTER INSERT ON {LEAF_CATALOG}
                 FOR EACH STATEMENT EXECUTE FUNCTION test_record_pool_write();"
        ))
        .execute(&database.pool)
        .await
        .expect("install the pool write log");
        let database_url = database_url_for(&database.config_url, database.database.name());
        let broker = PostgresBroker::connect_with(
            &PostgresConfig {
                database_url: with_application_name(&database_url, "reaper_runtime"),
                session_database_url: Some(with_application_name(&database_url, "reaper_session")),
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                worker_pool_pre_ping: false,
                pool_size: 30,
                max_overflow: 30,
                retain_rerun_input_default: false,
                pool_timeout: 30,
                pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
                echo: false,
                tcp_keepalives: Default::default(),
                tcp_user_timeout: Default::default(),
            },
            crate::ClientFamily::Rust,
        )
        .await
        .expect("connect a broker with two pools");
        let mut app_config = test_app_config(database.config_url.clone());
        app_config.recovery.auto_requeue_stale_claimed = true;
        let worker = worker_with_config(broker, app_config);
        let health = new_reaper_health();

        let reaper = worker.spawn_reaper_loop(Arc::clone(&health));
        tokio::time::timeout(Duration::from_secs(30), async {
            while health.read().await.partition_coverage.is_none() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the reaper completes a pass with partition maintenance");
        worker.cancel_token().cancel();
        tokio::time::timeout(Duration::from_secs(30), reaper)
            .await
            .expect("the reaper stops after cancel")
            .expect("reaper join");

        let writes: Vec<(String, String)> =
            sql::query_as("SELECT relation_name, application_name FROM test_pool_writes")
                .fetch_all(&database.pool)
                .await
                .expect("read the pool write log");
        let sessions_of = |relation: &str| -> Vec<&str> {
            writes
                .iter()
                .filter(|(name, _)| name == relation)
                .map(|(_, application)| application.as_str())
                .collect()
        };
        let task_writers = sessions_of("horsies_tasks");
        let catalog_writers = sessions_of(LEAF_CATALOG);
        assert!(
            !task_writers.is_empty(),
            "the requeue step updates horsies_tasks"
        );
        assert!(
            task_writers.iter().all(|name| *name == "reaper_runtime"),
            "{writes:?}"
        );
        assert!(
            !catalog_writers.is_empty(),
            "partition maintenance creates leaves"
        );
        assert!(
            catalog_writers.iter().all(|name| *name == "reaper_session"),
            "{writes:?}"
        );
        drop(worker);
        database.drop().await;
    }
}

#[cfg(test)]
mod held_task_tests;

#[cfg(test)]
mod claim_config_tests;

#[cfg(test)]
mod live_worker_tests;

#[cfg(test)]
mod start_tests;

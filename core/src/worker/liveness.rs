//! The worker liveness lock: the peers of a worker tell that its process
//! ended from a lock that no session holds any more.
//!
//! - Each worker process holds a session advisory lock on one dedicated
//!   connection to the session URL ([`LockSession`]). Its key is
//!   `horsies_worker_liveness_key(worker_id)`, that is
//!   `hashtextextended('horsies-worker-liveness:' || worker_id, 0)`. The
//!   worker id is a new UUIDv4 at each process start, so a restarted worker
//!   never holds the key of its earlier process.
//! - The worker takes the lock before its first claim and holds it until the
//!   end of its stop, after every task ended. Each acquisition writes a new
//!   generation for the worker id in the same transaction
//!   (`horsies_take_worker_liveness`), and the worker counts it only after
//!   that transaction committed. The holder probes the connection every
//!   [`LIVENESS_PROBE_INTERVAL`] with the listener's statement bound, and
//!   opens it again after a loss with the listener's reconnect backoff.
//! - A lock that another session holds counts as held: only this worker
//!   uses the key, so that session is an earlier session of this worker
//!   that the server has not ended yet. After a lock statement that failed
//!   or did not end, that session may hold the lock with no committed
//!   generation, so such a lock counts as held only once this worker took
//!   the lock again itself (`DroppedSessions`).
//! - A watchdog on its own OS thread reads the time since the start of the
//!   last statement that confirmed the lock. Past [`LIVENESS_REGAIN_BOUND`],
//!   with stale running recovery on and a task held, the worker fails closed
//!   as for a lost heartbeat: it kills every child that holds a task and ends
//!   the process with [`LIVENESS_LOCK_LOST_EXIT_CODE`], with no drain, before
//!   a peer can judge it gone. With no task held, no task is at risk: it runs
//!   on and keeps taking the lock again; a task that it starts while the
//!   lock is still not held makes it fail closed at the next watch.
//! - Peers judge an owner gone when two reaper reads at least
//!   [`OWNER_GONE_SPAN`] apart find its lock free at one generation
//!   ([`crate::worker::recovery`]). Each hold that the worker counts commits
//!   a generation while it lasts, so a regain between the two reads changes
//!   the generation. A worker that is alive but stalled keeps its
//!   connection, so it keeps its lock and is never judged gone.

use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;

use crate::core::config::recovery::RecoveryConfig;
use crate::core::PostgresConfig;
use crate::db::{
    parse_connection_url, ConnectPlan, ConnectPlanError, ConnectionEnvironment, EnvironmentError,
    LockSession, LockTake, SessionLockFailure, Transience, UrlError, SESSION_LOCK_RECONNECT_FIRST,
    SESSION_LOCK_RECONNECT_LIMIT, SESSION_LOCK_STATEMENT_BOUND,
};
use crate::worker::runner_beats::{kill_held_children, LiveDispatches, TaskCodeInWorker};
use crate::worker::thread_dispatch::with_starter_dispatch;

/// The longest a worker process runs a task without a confirmed hold of its
/// liveness lock: past it, the worker fails closed.
pub const LIVENESS_REGAIN_BOUND: Duration = Duration::from_secs(60);

/// What the peers add to [`LIVENESS_REGAIN_BOUND`] before they judge an
/// unheld owner gone: the watch interval of the worker, the kill of its
/// children (at most 5 s for their answers), its exit, and the reads of the
/// two passes.
pub const OWNER_GONE_MARGIN: Duration = Duration::from_secs(15);

/// The least time between the two reaper reads that judge an owner gone.
pub const OWNER_GONE_SPAN: Duration = LIVENESS_REGAIN_BOUND.saturating_add(OWNER_GONE_MARGIN);

/// How often the holder confirms the lock: one probe of its connection.
pub const LIVENESS_PROBE_INTERVAL: Duration = Duration::from_secs(5);

/// Exit code of the worker process when it failed closed because its
/// liveness lock was not held for [`LIVENESS_REGAIN_BOUND`] while it held a
/// task. No child uses it.
pub const LIVENESS_LOCK_LOST_EXIT_CODE: i32 = 79;

/// How often the watchdog reads the time since the last confirmation.
const WATCH_INTERVAL: Duration = Duration::from_millis(250);

/// A lock that is not confirmed is logged at most once per this time.
const UNCONFIRMED_LOG_INTERVAL: Duration = Duration::from_secs(60);

const WATCH_THREAD_NAME: &str = "horsies-liveness-watch";

/// The lock statement: one text parameter, the worker id. It takes the lock
/// with no wait and, when it took it, writes a new generation in the same
/// transaction; it answers whether it took the lock. An answer means that
/// the transaction committed: the driver returns the row after the server
/// reported the end of the statement's transaction.
const LIVENESS_LOCK_SQL: &str = "SELECT horsies_take_worker_liveness($1)";

/// The cause that the watchdog names while another session holds the lock
/// and a lock statement of this worker failed since its last acquisition.
const UNRECORDED_HOLDER_CAUSE: &str = "another session holds the lock, and a lock statement of \
     this worker failed since it last took the lock, so that session may hold it with no recorded \
     generation";

/// What the worker does when its lock is not confirmed past its regain
/// bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LivenessRecovery {
    /// Stale running recovery is on, so a peer recovers the RUNNING rows of
    /// a gone owner: with a task held, the worker fails closed.
    FailClosed,
    /// No reaper recovers a RUNNING row: a lock that is not held puts no
    /// task at risk, so the worker logs it and runs on.
    LogOnly,
}

impl LivenessRecovery {
    /// The rule of the worker's recovery config: the peers recover RUNNING
    /// rows of a gone owner only with `auto_fail_stale_running`.
    pub fn from_config(recovery: &RecoveryConfig) -> Self {
        match recovery.auto_fail_stale_running {
            true => Self::FailClosed,
            false => Self::LogOnly,
        }
    }
}

/// The rule of one worker's liveness lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LivenessRule {
    /// The time without a confirmed hold after which the worker acts.
    pub regain_bound: Duration,
    /// The time between two confirmations of a held lock; well below the
    /// regain bound.
    pub probe_interval: Duration,
    pub recovery: LivenessRecovery,
}

impl LivenessRule {
    /// The rule of a worker with this recovery config:
    /// [`LIVENESS_REGAIN_BOUND`], [`LIVENESS_PROBE_INTERVAL`] and
    /// [`LivenessRecovery::from_config`].
    pub fn of_config(recovery: &RecoveryConfig) -> Self {
        Self {
            regain_bound: LIVENESS_REGAIN_BOUND,
            probe_interval: LIVENESS_PROBE_INTERVAL,
            recovery: LivenessRecovery::from_config(recovery),
        }
    }
}

/// The connection plan of the liveness lock was refused. Nothing was
/// connected.
#[derive(Debug, thiserror::Error)]
pub enum LivenessPlanError {
    #[error("the connection environment could not be read: {0}")]
    Environment(EnvironmentError),
    #[error("the session URL is refused: {0}")]
    Url(UrlError),
    #[error("the connection plan of the session URL could not be loaded: {0}")]
    Plan(ConnectPlanError),
}

/// The plan of the liveness lock connection: the session URL of `config`
/// with its socket settings, and the process connection environment.
///
/// Reads the connection environment and the files that the URL names.
/// Connects to nothing.
///
/// Errors: [`LivenessPlanError`].
pub fn liveness_connect_plan(config: &PostgresConfig) -> Result<ConnectPlan, LivenessPlanError> {
    let environment = ConnectionEnvironment::capture().map_err(LivenessPlanError::Environment)?;
    let settings = parse_connection_url(config.effective_session_database_url(), &environment)
        .map_err(LivenessPlanError::Url)?;
    ConnectPlan::load(&settings, &environment, &config.socket_settings())
        .map_err(LivenessPlanError::Plan)
}

/// The liveness lock of a worker did not start. No lock is held, and no
/// holder or watchdog runs.
#[derive(Debug, thiserror::Error)]
pub enum LivenessStartError {
    #[error("the liveness lock connection plan is refused: {0}")]
    Plan(LivenessPlanError),
    #[error("the liveness lock could not be taken: {0}")]
    Lock(SessionLockFailure),
    #[error("the liveness watchdog thread did not start: {0}")]
    WatchThread(std::io::Error),
}

impl LivenessStartError {
    /// Whether a later start can succeed: a lock failure that may clear.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Plan(_) | Self::WatchThread(_) => false,
            Self::Lock(failure) => match failure.transience() {
                Transience::Transient => true,
                Transience::Permanent => false,
            },
        }
    }
}

/// Whether a session that this worker opened earlier may hold the lock with
/// no committed generation. A peer can miss such a hold between two reads
/// that see one generation, so an answer "held by another session" confirms
/// the lock only when every such session recorded its acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DroppedSessions {
    /// Every session of this worker that may hold the lock committed the
    /// generation of its acquisition.
    Recorded,
    /// A lock statement of this worker failed or did not end since its last
    /// acquisition: its session may hold the lock with no committed
    /// generation until the server ends that session.
    MayHoldUnrecorded,
}

/// What one answer of the lock statement means for the hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TakeVerdict {
    /// The answer confirms the lock.
    Confirms,
    /// Another session holds the lock, and it may hold it with no recorded
    /// generation: the answer confirms nothing.
    Unconfirmed,
}

impl DroppedSessions {
    /// The state after the lock statement answered `take`, and what the
    /// answer means. Pure.
    ///
    /// - `Taken`: this session took the lock and its generation committed;
    ///   no other session holds the lock, so no earlier session does.
    /// - `HeldElsewhere`: an earlier session of this worker holds the lock.
    ///   It confirms the lock only when that session recorded its
    ///   acquisition.
    fn after_answer(self, take: LockTake) -> (DroppedSessions, TakeVerdict) {
        match (take, self) {
            (LockTake::Taken, _) => (DroppedSessions::Recorded, TakeVerdict::Confirms),
            (LockTake::HeldElsewhere, DroppedSessions::Recorded) => {
                (DroppedSessions::Recorded, TakeVerdict::Confirms)
            }
            (LockTake::HeldElsewhere, DroppedSessions::MayHoldUnrecorded) => {
                (DroppedSessions::MayHoldUnrecorded, TakeVerdict::Unconfirmed)
            }
        }
    }

    /// The state after a lock statement (an opening or a new attempt on a
    /// session) failed. A failed connection opened no session; any other
    /// failure may have left its session with the lock and no committed
    /// generation. Pure.
    fn after_failed_take(self, failure: &SessionLockFailure) -> DroppedSessions {
        match failure {
            SessionLockFailure::Connect(_) => self,
            SessionLockFailure::Statement { .. }
            | SessionLockFailure::TimedOut { .. }
            | SessionLockFailure::Answer(_) => DroppedSessions::MayHoldUnrecorded,
        }
    }
}

/// Why the lock is not confirmed now.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Hold {
    /// The last statement confirmed the lock.
    Confirmed,
    /// The session was lost, or a new one could not take the lock.
    Lost { cause: String },
}

/// What the holder last confirmed, shared with the watchdog thread.
#[derive(Debug, Clone)]
struct Confirmation {
    /// The start of the last statement that confirmed the lock held.
    confirmed_at: Instant,
    hold: Hold,
}

#[derive(Clone)]
struct SharedConfirmation(Arc<Mutex<Confirmation>>);

impl SharedConfirmation {
    fn new(confirmed_at: Instant) -> Self {
        Self(Arc::new(Mutex::new(Confirmation {
            confirmed_at,
            hold: Hold::Confirmed,
        })))
    }

    fn read(&self) -> Confirmation {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// A statement that started at `started` confirmed the lock.
    fn confirm(&self, started: Instant) {
        let mut confirmation = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        *confirmation = Confirmation {
            confirmed_at: confirmation.confirmed_at.max(started),
            hold: Hold::Confirmed,
        };
    }

    /// The lock is not confirmed since the last confirmation, for `cause`.
    fn lose(&self, cause: String) {
        let mut confirmation = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        confirmation.hold = Hold::Lost { cause };
    }
}

/// Whether the liveness lock is held now, as the holder last found it.
#[cfg(feature = "metrics-endpoint")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LockHold {
    /// The last lock statement confirmed the lock.
    Held,
    /// The session was lost, or a new one could not take the lock; `cause`
    /// names why. The holder takes the lock again with the reconnect
    /// backoff.
    NotHeld { cause: String },
}

/// A read of what the holder of the liveness lock last found: the readiness
/// probe of the worker endpoint reads it. Clones read one holder.
#[cfg(feature = "metrics-endpoint")]
#[derive(Clone)]
pub(crate) struct LockHoldView(SharedConfirmation);

#[cfg(feature = "metrics-endpoint")]
impl LockHoldView {
    /// Whether the lock is held now. Takes the lock of the shared record for
    /// one copy; never waits on the database.
    pub(crate) fn hold(&self) -> LockHold {
        match self.0.read().hold {
            Hold::Confirmed => LockHold::Held,
            Hold::Lost { cause } => LockHold::NotHeld { cause },
        }
    }

    /// A view of a lock held now, for the tests of its readers.
    #[cfg(test)]
    pub(crate) fn held_for_test() -> Self {
        Self(SharedConfirmation::new(Instant::now()))
    }

    /// A view of a lock lost for `cause`, for the tests of its readers.
    #[cfg(test)]
    pub(crate) fn lost_for_test(cause: &str) -> Self {
        let shared = SharedConfirmation::new(Instant::now());
        shared.lose(cause.to_owned());
        Self(shared)
    }
}

/// Whether the worker holds a task whose row a peer could recover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TasksAtRisk {
    Held,
    NoneHeld,
}

/// What one watch found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchVerdict {
    /// The lock was confirmed within the regain bound.
    WithinBound,
    /// Past the bound, and nothing is at risk (no task held, or no reaper
    /// recovers RUNNING rows): log it and run on.
    PastBoundRunOn,
    /// Past the bound with a task held, and a peer may recover its row.
    FailClosed,
}

/// The verdict of one watch. Pure.
fn watch_verdict(unconfirmed: Duration, rule: LivenessRule, tasks: TasksAtRisk) -> WatchVerdict {
    match (unconfirmed > rule.regain_bound, rule.recovery, tasks) {
        (false, _, _) => WatchVerdict::WithinBound,
        (true, LivenessRecovery::FailClosed, TasksAtRisk::Held) => WatchVerdict::FailClosed,
        (true, LivenessRecovery::FailClosed, TasksAtRisk::NoneHeld)
        | (true, LivenessRecovery::LogOnly, _) => WatchVerdict::PastBoundRunOn,
    }
}

/// What the watchdog acts on.
struct WatchContext {
    shared: SharedConfirmation,
    rule: LivenessRule,
    live: LiveDispatches,
}

/// A running liveness lock: its holder task, its watchdog thread, and the
/// signal that the holder closed its connection.
pub(crate) struct RunningLivenessLock {
    /// What the holder last found, for the readiness probe.
    #[cfg(feature = "metrics-endpoint")]
    shared: SharedConfirmation,
    holder_cancel: CancellationToken,
    released: oneshot::Receiver<()>,
    watch_stop: Sender<()>,
    watch: std::thread::JoinHandle<()>,
}

/// A started liveness lock, and the holder task that the worker supervises
/// with its other service loops.
pub(crate) struct StartedLivenessLock {
    pub(crate) running: RunningLivenessLock,
    pub(crate) holder: JoinHandle<()>,
}

/// Take the liveness lock of `worker_id` on a new connection of `plan` (one
/// attempt), then start its holder on the current runtime and its watchdog
/// on its own thread. The watchdog acts by `rule` on the tasks of `live`.
///
/// Errors: [`LivenessStartError::Lock`] when the connection or the lock
/// statement failed (its transience tells whether a later attempt can
/// succeed); [`LivenessStartError::WatchThread`] when the OS refused the
/// thread. On an error the connection is closed, so no lock is held.
pub(crate) async fn start_liveness_lock(
    plan: Arc<ConnectPlan>,
    worker_id: &str,
    rule: LivenessRule,
    live: LiveDispatches,
) -> Result<StartedLivenessLock, LivenessStartError> {
    let started = Instant::now();
    let (session, take) = LockSession::open(&plan, LIVENESS_LOCK_SQL, worker_id.to_owned())
        .await
        .map_err(LivenessStartError::Lock)?;
    // An earlier start of this process may have left a session that holds
    // the lock: until this worker takes the lock itself, a lock held
    // elsewhere confirms nothing. The start counts as the first
    // confirmation in both cases: the worker claims no row before the start
    // returns, so every peer read that returns this worker comes after it.
    let (dropped, _start_verdict) = DroppedSessions::MayHoldUnrecorded.after_answer(take);
    let shared = SharedConfirmation::new(started);
    let (watch_stop, stop_received) = std::sync::mpsc::channel::<()>();
    let context = WatchContext {
        shared: shared.clone(),
        rule,
        live,
    };
    let watch = std::thread::Builder::new()
        .name(WATCH_THREAD_NAME.to_owned())
        .spawn(with_starter_dispatch(move || {
            run_watch(&context, &stop_received);
        }))
        .map_err(LivenessStartError::WatchThread)?;
    tracing::info!(
        worker_id,
        held = ?take,
        regain_bound_s = rule.regain_bound.as_secs_f64(),
        "the worker liveness lock is held",
    );
    #[cfg(feature = "metrics-endpoint")]
    let view = shared.clone();
    let holder_cancel = CancellationToken::new();
    let (released_sender, released) = oneshot::channel();
    let holder = tokio::spawn(
        hold_then_release(
            Holder {
                plan,
                worker_id: worker_id.to_owned(),
                probe_interval: rule.probe_interval,
                shared,
                cancel: holder_cancel.clone(),
            },
            session,
            take,
            dropped,
            released_sender,
        )
        .with_current_subscriber(),
    );
    Ok(StartedLivenessLock {
        running: RunningLivenessLock {
            #[cfg(feature = "metrics-endpoint")]
            shared: view,
            holder_cancel,
            released,
            watch_stop,
            watch,
        },
        holder,
    })
}

impl RunningLivenessLock {
    /// A read of what the holder last found, for the readiness probe.
    #[cfg(feature = "metrics-endpoint")]
    pub(crate) fn hold_view(&self) -> LockHoldView {
        LockHoldView(self.shared.clone())
    }

    /// Stop the watchdog, then the holder, and wait at most the statement
    /// bound for its connection to close: the server then releases the lock.
    /// Call it after every task of the worker ended.
    pub(crate) async fn stop(self) {
        // A send error means the watchdog already ended.
        let _sent = self.watch_stop.send(());
        let watch = self.watch;
        match tokio::task::spawn_blocking(move || watch.join()).await {
            Ok(Ok(())) => {}
            Ok(Err(_panic)) => {
                tracing::error!("the liveness watchdog thread panicked");
            }
            Err(error) => {
                tracing::error!(
                    %error,
                    "joining the liveness watchdog thread failed: {error}",
                );
            }
        }
        self.holder_cancel.cancel();
        match tokio::time::timeout(SESSION_LOCK_STATEMENT_BOUND, self.released).await {
            Ok(Ok(())) => {}
            Ok(Err(_ended)) => {
                tracing::warn!(
                    "the liveness lock holder ended before its stop; its connection is closed",
                );
            }
            Err(_elapsed) => {
                tracing::warn!(
                    bound_s = SESSION_LOCK_STATEMENT_BOUND.as_secs_f64(),
                    "the liveness lock holder did not end within its bound; the lock is released \
                     when its connection closes",
                );
            }
        }
    }
}

/// What the holder needs across sessions.
struct Holder {
    plan: Arc<ConnectPlan>,
    worker_id: String,
    probe_interval: Duration,
    shared: SharedConfirmation,
    cancel: CancellationToken,
}

/// How one held session ended.
enum HoldEnd {
    /// The stop was asked for.
    Stopped,
    /// The session was lost, for this cause.
    Lost(String),
}

/// Hold the lock until the stop, then report that the connection is closed.
async fn hold_then_release(
    holder: Holder,
    session: LockSession,
    take: LockTake,
    dropped: DroppedSessions,
    released: oneshot::Sender<()>,
) {
    hold(&holder, session, take, dropped).await;
    // The receiver is gone when the stop no longer waits.
    let _reported = released.send(());
}

/// Hold the lock: probe the session; after a loss, open a new one with the
/// reconnect backoff, until the stop. Each session is closed when it is
/// dropped. `dropped` starts as the state of the first session's answer.
async fn hold(holder: &Holder, first: LockSession, first_take: LockTake, dropped: DroppedSessions) {
    let mut held = Some((first, first_take));
    let mut dropped = dropped;
    let mut delay = SESSION_LOCK_RECONNECT_FIRST;
    loop {
        match held.take() {
            Some((mut session, take)) => {
                match hold_session(holder, &mut session, take, &mut dropped).await {
                    HoldEnd::Stopped => return,
                    HoldEnd::Lost(cause) => {
                        tracing::warn!(
                            worker_id = %holder.worker_id,
                            %cause,
                            "the worker liveness lock was lost: {cause}; taking it again",
                        );
                        holder.shared.lose(cause);
                        delay = SESSION_LOCK_RECONNECT_FIRST;
                    }
                }
            }
            None => {
                tokio::select! {
                    () = holder.cancel.cancelled() => return,
                    () = tokio::time::sleep(delay) => {}
                }
                let started = Instant::now();
                let opened = tokio::select! {
                    () = holder.cancel.cancelled() => return,
                    opened = LockSession::open(
                        &holder.plan,
                        LIVENESS_LOCK_SQL,
                        holder.worker_id.clone(),
                    ) => opened,
                };
                match opened {
                    Ok((session, take)) => {
                        let (next, verdict) = dropped.after_answer(take);
                        dropped = next;
                        match verdict {
                            TakeVerdict::Confirms => {
                                holder.shared.confirm(started);
                                tracing::info!(
                                    worker_id = %holder.worker_id,
                                    held = ?take,
                                    "the worker liveness lock is held again",
                                );
                            }
                            TakeVerdict::Unconfirmed => {
                                holder.shared.lose(UNRECORDED_HOLDER_CAUSE.to_owned());
                            }
                        }
                        held = Some((session, take));
                    }
                    Err(failure) => {
                        dropped = dropped.after_failed_take(&failure);
                        holder.shared.lose(failure.to_string());
                        delay = delay.saturating_mul(2).min(SESSION_LOCK_RECONNECT_LIMIT);
                    }
                }
            }
        }
    }
}

/// Hold one session: every probe interval of the rule, confirm the lock
/// (a probe while this session holds it; the lock statement again while an
/// earlier session holds it, read by `dropped`), until the stop or a loss.
async fn hold_session(
    holder: &Holder,
    session: &mut LockSession,
    take: LockTake,
    dropped: &mut DroppedSessions,
) -> HoldEnd {
    let mut take = take;
    loop {
        tokio::select! {
            () = holder.cancel.cancelled() => return HoldEnd::Stopped,
            end = session.ended() => return HoldEnd::Lost(end.to_string()),
            () = tokio::time::sleep(holder.probe_interval) => {}
        }
        let started = Instant::now();
        match take {
            LockTake::Taken => match session.probe().await {
                Ok(()) => holder.shared.confirm(started),
                Err(failure) => return HoldEnd::Lost(failure.to_string()),
            },
            LockTake::HeldElsewhere => match session.take().await {
                Ok(now_held) => {
                    let (next, verdict) = dropped.after_answer(now_held);
                    *dropped = next;
                    match verdict {
                        TakeVerdict::Confirms => holder.shared.confirm(started),
                        TakeVerdict::Unconfirmed => {
                            holder.shared.lose(UNRECORDED_HOLDER_CAUSE.to_owned());
                        }
                    }
                    take = now_held;
                }
                Err(failure) => {
                    *dropped = dropped.after_failed_take(&failure);
                    return HoldEnd::Lost(failure.to_string());
                }
            },
        }
    }
}

/// The watchdog thread body: one verdict per [`WATCH_INTERVAL`] until the
/// stop.
fn run_watch(context: &WatchContext, stop: &Receiver<()>) {
    let mut last_log: Option<Instant> = None;
    loop {
        match stop.recv_timeout(WATCH_INTERVAL) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {}
        }
        let confirmation = context.shared.read();
        let now = Instant::now();
        let unconfirmed = now.saturating_duration_since(confirmation.confirmed_at);
        let tasks = match context.live.holds_any() {
            true => TasksAtRisk::Held,
            false => TasksAtRisk::NoneHeld,
        };
        match watch_verdict(unconfirmed, context.rule, tasks) {
            WatchVerdict::WithinBound => last_log = None,
            WatchVerdict::PastBoundRunOn => {
                let due = last_log
                    .is_none_or(|at| now.saturating_duration_since(at) >= UNCONFIRMED_LOG_INTERVAL);
                if due {
                    last_log = Some(now);
                    tracing::warn!(
                        unconfirmed_s = unconfirmed.as_secs_f64(),
                        regain_bound_s = context.rule.regain_bound.as_secs_f64(),
                        cause = %describe(&confirmation.hold),
                        "the worker liveness lock is not held; no task is at risk, so the worker \
                         runs on and takes the lock again",
                    );
                }
            }
            WatchVerdict::FailClosed => fail_closed(context, unconfirmed, &confirmation.hold),
        }
    }
}

fn describe(hold: &Hold) -> String {
    match hold {
        Hold::Confirmed => "the last statement confirmed it".to_owned(),
        Hold::Lost { cause } => cause.clone(),
    }
}

/// The lock was not confirmed for `unconfirmed`, past the regain bound,
/// while a task is held: a peer may judge this worker gone and recover the
/// task. Close the registry, kill every child that holds a task, and end
/// the process at once, with no drain. A panic while killing also ends the
/// process.
fn fail_closed(context: &WatchContext, unconfirmed: Duration, hold: &Hold) -> ! {
    tracing::error!(
        exit_code = LIVENESS_LOCK_LOST_EXIT_CODE,
        unconfirmed_s = unconfirmed.as_secs_f64(),
        regain_bound_s = context.rule.regain_bound.as_secs_f64(),
        cause = %describe(hold),
        "worker failed closed: its liveness lock is not held past its regain bound; every child \
         that holds a task is killed and the process ends, so no peer recovers a task that \
         still runs",
    );
    match std::panic::catch_unwind(AssertUnwindSafe(|| kill_held_children(&context.live))) {
        Ok(TaskCodeInWorker::Held | TaskCodeInWorker::NoneHeld) => {}
        Err(_panic) => {
            tracing::error!("killing the children panicked; ending the process");
        }
    }
    std::process::exit(LIVENESS_LOCK_LOST_EXIT_CODE)
}

#[cfg(test)]
mod tests;

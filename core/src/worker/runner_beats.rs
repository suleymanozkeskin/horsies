//! Runner heartbeats of the worker parent.
//!
//! - The writer beats every task the worker holds, once per
//!   `runner_heartbeat_interval_ms`, in one fenced statement, on its own
//!   connection outside the shared pool, so a busy pool cannot starve it.
//! - A beat lands only on a row that is RUNNING, owned by this worker, at the
//!   claim generation of the dispatch, and without the finalizing marker.
//! - With `auto_fail_stale_running` on, a watchdog on its own OS thread fails
//!   closed when no beat lands for half of `running_stale_threshold_ms`, so
//!   no task runs without a beat when the reaper fails its row. It closes the
//!   registry, kills every child that holds a task, and asks the worker to
//!   stop; the process exits with [`RUNNER_HEARTBEAT_LOST_EXIT_CODE`] when
//!   the worker has not stopped within [`FAIL_CLOSED_EXIT_GRACE`]. The
//!   watchdog does not depend on the async runtime: a blocked runtime cannot
//!   keep it from acting.
//! - With it off, no reaper fails a RUNNING row: late beats are logged and
//!   the worker runs on. A writer that dies is logged once, and the worker
//!   runs on without runner beats.
//! - The registry of held dispatches also routes an operator cancel to the
//!   slot that holds the run ([`LiveDispatches::request_cancel`]). A
//!   dispatch whose task was cancelled gets no beat: the cancel moved its
//!   row to history. A held dispatch whose beat did not land is a beat miss
//!   ([`LiveDispatches::next_beat_misses`]): the cancel router reads whether
//!   an operator cancelled it, for a cancel notice that the listener lost.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tokio::sync::{oneshot, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;
use uuid::Uuid;

use crate::broker::cancelled_runs::RunClaim;
use crate::broker::core_rows::column;
use crate::broker::pools::PoolSettingsError;
use crate::core::config::recovery::{RecoveryConfig, FAIL_CLOSED_WINDOW_DIVISOR};
use crate::core::PostgresConfig;
use crate::db::{
    parse_connection_url, CheckoutClass, ConnectPlan, ConnectionAge, ConnectionEnvironment,
    CorePool, IdlePing, ParameterType, PoolPlan, PoolRole, PoolSizing, Reserve, Row,
    SocketSettings, StatementBound, StatementFailure, StatementProtocol, TypedStatement,
};
use crate::worker::executor::protocol::ProcessId;
use crate::worker::executor::supervisor::{ChildId, KillOutcome, SpawnerHandle, WorkerKillMark};
use crate::worker::thread_dispatch::with_starter_dispatch;

/// Exit code of the worker process when it failed closed because its runner
/// heartbeats stopped. No other horsies process uses it. The core parent
/// owns exit codes 76 and 77, and 79 for a lost liveness lock
/// ([`crate::worker::liveness::LIVENESS_LOCK_LOST_EXIT_CODE`]).
pub const RUNNER_HEARTBEAT_LOST_EXIT_CODE: i32 = 77;

/// How often the watchdog reads the time since the last beat.
const WATCH_INTERVAL: Duration = Duration::from_millis(250);

/// After it failed closed and asked the worker to stop, the watchdog waits
/// this long for the stop, then ends the process (Python
/// `FAIL_CLOSED_EXIT_GRACE_S`).
pub const FAIL_CLOSED_EXIT_GRACE: Duration = Duration::from_secs(120);

/// The longest wait for the spawner's answers to the kills of a fail-closed
/// stop. The kills are queued at once; the answers only feed the log.
const FAIL_CLOSED_KILL_ANSWER_WAIT: Duration = Duration::from_secs(5);

/// How often the watchdog looks for those answers.
const KILL_ANSWER_POLL: Duration = Duration::from_millis(10);

/// The fail-closed window ends at least this long before the reaper may count
/// the last beat as stale: two watch intervals. It is below the smallest beat
/// interval (1 s), so a beat that lands on time never shortens the window.
const FAIL_CLOSED_KILL_MARGIN: Duration = Duration::from_millis(500);

/// Wait before the first retry after a failed write; it doubles per failure,
/// at most [`BEAT_RETRY_DOUBLINGS_MAX`] times, up to [`BEAT_RETRY_MAX_DELAY`]
/// and never above one beat interval.
const BEAT_RETRY_FIRST_DELAY: Duration = Duration::from_millis(250);
const BEAT_RETRY_DOUBLINGS_MAX: u32 = 5;
const BEAT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

/// Failed writes and late beats are logged at most once per this time.
const BEAT_FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// With stale running recovery off, a beat is late after this many intervals.
const LATE_BEAT_INTERVALS: u32 = 2;

const WATCH_THREAD_NAME: &str = "horsies-runner-heartbeat-watch";

/// The start of the watchdog's line for a dead writer with stale running
/// recovery off (Python `describe_fail_closed(BeatThreadDied)`).
const DEAD_WRITER_LINE: &str = "the runner heartbeat writer died";

/// One statement per interval for every held dispatch. Each row is fenced
/// like every other write of a dispatch: this worker, and the claim
/// generation of the dispatch. A row with the finalizing marker gets no beat:
/// its attempt has ended, and the reaper reads the marker for that row.
/// Returns the task id and the claim generation of each dispatch whose beat
/// landed; the insert runs to its end whatever the outer select reads.
const INSERT_RUNNER_HEARTBEATS_SQL: &str = "\
WITH due AS (
    SELECT t.id, d.claimed_at
    FROM unnest($4::uuid[], $5::timestamptz[]) AS d(task_id, claimed_at)
    JOIN horsies_tasks t ON t.id = d.task_id
    WHERE t.status = 'RUNNING'
      AND t.claimed_by_worker_id = $1::varchar
      AND t.claimed_at = d.claimed_at
      AND t.finalizing_at IS NULL
), landed AS (
    INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
    SELECT due.id, $1::varchar, 'runner', NOW(), $2::varchar, $3::integer
    FROM due
    RETURNING task_id
)
SELECT due.id AS task_id, due.claimed_at AS claimed_at
FROM due
JOIN landed ON landed.task_id = due.id";

/// [`INSERT_RUNNER_HEARTBEATS_SQL`] with its parameter types: worker id,
/// hostname, pid, task ids, claim generations.
static INSERT_RUNNER_HEARTBEATS: TypedStatement = TypedStatement::new(
    "insert_runner_heartbeats",
    INSERT_RUNNER_HEARTBEATS_SQL,
    &[
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::UuidArray,
        ParameterType::TimestamptzArray,
    ],
);

/// The bound of one connect of the beat connection, in place of the URL's
/// `connect_timeout` (Python `BEAT_CONNECT_TIMEOUT_S`). Only the writer
/// checks out from the beat pool, one statement at a time, so a checkout
/// waits for no other user; the same time bounds the checkout.
pub const BEAT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The child process of the executor that runs a held dispatch: what the
/// fail-closed watchdog needs to mark it and kill its session, and the bell
/// of the slot that serves it, which an operator cancel of the dispatch
/// rings.
#[derive(Clone)]
pub struct HeldChild {
    pub id: ChildId,
    pub pid: ProcessId,
    pub spawner: SpawnerHandle,
    pub kill_mark: WorkerKillMark,
    pub cancel_bell: CancelBell,
}

/// Wakes the slot of a held dispatch when an operator cancel is routed to
/// the dispatch. A ring while the slot does not wait is kept: the next
/// [`CancelBell::rung`] completes at once. A slot that wakes reads which of
/// its dispatches have a cancel with [`HeldDispatch::take_cancel`].
#[derive(Clone, Default)]
pub struct CancelBell(Arc<Notify>);

impl CancelBell {
    fn ring(&self) {
        self.0.notify_one();
    }

    /// Complete at the next ring, or at once when a ring came since the
    /// last wait completed.
    pub async fn rung(&self) {
        self.0.notified().await;
    }
}

/// Whether an operator cancel was routed to a held dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelMark {
    NotRequested,
    /// The router routed a cancel; the slot has not taken it.
    Requested,
    /// The slot took the cancel.
    Taken,
}

/// One dispatch the registry holds.
#[derive(Clone)]
struct LiveDispatch {
    task_id: Uuid,
    /// The claim generation; the beat fences on it.
    claimed_at: DateTime<Utc>,
    child: HeldChild,
    cancel: CancelMark,
}

/// What [`LiveDispatches::request_cancel`] did with a cancel of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelRouting {
    /// The dispatch of the run is held: its slot was told.
    Routed,
    /// No dispatch of this task at this claim generation is held: it ended,
    /// it runs on another worker, or the registry was closed.
    NotHeld,
    /// A cancel of the run was routed already.
    AlreadyRequested,
}

/// Whether a held dispatch has an operator cancel that its slot has not
/// taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelTake {
    /// A cancel was routed to the dispatch; this read took it. Later reads
    /// give `NoRequest`.
    Requested,
    NoRequest,
}

/// Whether the registry takes new dispatches (Python executor design,
/// section 8.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegistryState {
    Open,
    /// The worker failed closed: no beat would cover a new dispatch.
    Closed,
}

struct Registry {
    state: RegistryState,
    /// By ticket. A ticket is never reused, so the guard of an ended
    /// dispatch cannot remove a later dispatch of the same task.
    held: BTreeMap<u64, LiveDispatch>,
    next_ticket: u64,
    /// The held dispatches with no cancel whose beat did not land in the
    /// last beat pass, until the cancel router takes them. At most the held
    /// dispatches.
    beat_misses: Vec<RunClaim>,
}

/// The runner heartbeats failed closed: no beat covers a new dispatch. The
/// caller does not start the task; a child that would run it is killed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the runner heartbeats failed closed; no beat would cover a new task")]
pub struct RegistryClosed;

/// The dispatches this worker holds: task id, claim generation and the
/// child that runs each, with the operator cancel of each.
///
/// A dispatch is held from its send until its end is read, a kill is sent
/// to its child, or its child's exit is seen; the [`HeldDispatch`] guard
/// removes it when dropped. One lock guards every read and write; no caller
/// holds it across I/O.
#[derive(Clone)]
pub struct LiveDispatches {
    registry: Arc<Mutex<Registry>>,
    /// Rung when a beat pass noted misses.
    beat_misses_noted: Arc<Notify>,
}

impl Default for LiveDispatches {
    fn default() -> Self {
        Self {
            registry: Arc::new(Mutex::new(Registry {
                state: RegistryState::Open,
                held: BTreeMap::new(),
                next_ticket: 1,
                beat_misses: Vec::new(),
            })),
            beat_misses_noted: Arc::new(Notify::new()),
        }
    }
}

/// One held dispatch. Dropping it ends the runner beats of the task.
pub struct HeldDispatch {
    live: LiveDispatches,
    ticket: u64,
}

impl LiveDispatches {
    fn lock(&self) -> MutexGuard<'_, Registry> {
        // A poisoned lock means a panic while the registry was held; its
        // map and state are still valid values, so they are used as they are.
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Start beating `task_id` at the claim generation `claimed_at`, run by
    /// `child`.
    ///
    /// `Err(RegistryClosed)` after the watchdog failed closed: the caller
    /// must not start the task, because no beat would cover it.
    pub fn hold(
        &self,
        task_id: Uuid,
        claimed_at: DateTime<Utc>,
        child: HeldChild,
    ) -> Result<HeldDispatch, RegistryClosed> {
        let mut registry = self.lock();
        match registry.state {
            RegistryState::Closed => return Err(RegistryClosed),
            RegistryState::Open => {}
        }
        let ticket = registry.next_ticket;
        registry.next_ticket += 1;
        registry.held.insert(
            ticket,
            LiveDispatch {
                task_id,
                claimed_at,
                child,
                cancel: CancelMark::NotRequested,
            },
        );
        Ok(HeldDispatch {
            live: self.clone(),
            ticket,
        })
    }

    /// The held task ids and their claim generations, in matching order:
    /// the dispatches that the beats cover. A dispatch with an operator
    /// cancel is left out: the cancel moved its row to history. Empty after
    /// [`LiveDispatches::close`].
    fn snapshot(&self) -> (Vec<Uuid>, Vec<DateTime<Utc>>) {
        self.lock()
            .held
            .values()
            .filter(|dispatch| dispatch.cancel == CancelMark::NotRequested)
            .map(|dispatch| (dispatch.task_id, dispatch.claimed_at))
            .unzip()
    }

    /// Route an operator cancel of the run of `task_id` at the claim
    /// generation `claimed_at` to the slot that holds its dispatch: mark the
    /// dispatch and ring the bell of its slot. A dispatch of the same task
    /// at another claim generation is another run, and is not marked.
    pub fn request_cancel(&self, task_id: Uuid, claimed_at: DateTime<Utc>) -> CancelRouting {
        let mut registry = self.lock();
        let Some(dispatch) = registry
            .held
            .values_mut()
            .find(|dispatch| dispatch.task_id == task_id && dispatch.claimed_at == claimed_at)
        else {
            return CancelRouting::NotHeld;
        };
        match dispatch.cancel {
            CancelMark::NotRequested => {
                dispatch.cancel = CancelMark::Requested;
                dispatch.child.cancel_bell.ring();
                CancelRouting::Routed
            }
            CancelMark::Requested | CancelMark::Taken => CancelRouting::AlreadyRequested,
        }
    }

    /// The runs of the held dispatches with no cancel: the runs whose cancel
    /// the cancel router reads after the listener lost notices. At most the
    /// held dispatches.
    pub fn cancel_candidates(&self) -> Vec<RunClaim> {
        self.lock()
            .held
            .values()
            .filter(|dispatch| dispatch.cancel == CancelMark::NotRequested)
            .map(|dispatch| RunClaim {
                task_id: dispatch.task_id,
                claimed_at: dispatch.claimed_at,
            })
            .collect()
    }

    /// Note the beat pass that sent `sent` and landed `landed`: the runs of
    /// `sent` that are still held with no cancel and did not land are its
    /// misses. They replace the misses of the last pass that the router did
    /// not take; when there are any, the router is woken.
    fn note_beat_misses(&self, sent: &[RunClaim], landed: &[RunClaim]) {
        let mut registry = self.lock();
        let misses: Vec<RunClaim> = sent
            .iter()
            .filter(|run| !landed.contains(*run))
            .filter(|run| {
                registry.held.values().any(|dispatch| {
                    dispatch.task_id == run.task_id
                        && dispatch.claimed_at == run.claimed_at
                        && dispatch.cancel == CancelMark::NotRequested
                })
            })
            .copied()
            .collect();
        let noted = !misses.is_empty();
        registry.beat_misses = misses;
        drop(registry);
        match noted {
            true => self.beat_misses_noted.notify_one(),
            false => {}
        }
    }

    /// Wait for the misses of a beat pass, and take them: the held runs
    /// with no cancel whose beat did not land. A pass whose misses were
    /// replaced by a later pass before this read gives the later misses;
    /// empty when every miss ended or was cancelled since.
    pub async fn next_beat_misses(&self) -> Vec<RunClaim> {
        self.beat_misses_noted.notified().await;
        std::mem::take(&mut self.lock().beat_misses)
    }

    /// Whether a task is held now: a task whose row a peer must not recover
    /// while it runs.
    pub(crate) fn holds_any(&self) -> bool {
        !self.lock().held.is_empty()
    }

    /// The ids of the held tasks, in ticket order, for tests that check
    /// what the beats cover.
    #[cfg(test)]
    pub(crate) fn held_task_ids(&self) -> Vec<Uuid> {
        self.snapshot().0
    }

    /// The pid of the child of each held dispatch, in ticket order, for
    /// tests that check which child the beats cover.
    #[cfg(test)]
    pub(crate) fn held_child_pids(&self) -> Vec<ProcessId> {
        self.lock()
            .held
            .values()
            .map(|dispatch| dispatch.child.pid)
            .collect()
    }

    /// Refuse every later hold, and return and remove what is held, in
    /// ticket order. A second call returns nothing.
    fn close(&self) -> Vec<LiveDispatch> {
        let mut registry = self.lock();
        registry.state = RegistryState::Closed;
        std::mem::take(&mut registry.held).into_values().collect()
    }
}

impl HeldDispatch {
    /// Take the operator cancel that the router routed to this dispatch,
    /// once: `Requested` for the first read after the route, `NoRequest`
    /// for every other read, and after the registry was closed.
    pub fn take_cancel(&self) -> CancelTake {
        let mut registry = self.live.lock();
        let Some(dispatch) = registry.held.get_mut(&self.ticket) else {
            return CancelTake::NoRequest;
        };
        match dispatch.cancel {
            CancelMark::Requested => {
                dispatch.cancel = CancelMark::Taken;
                CancelTake::Requested
            }
            CancelMark::NotRequested | CancelMark::Taken => CancelTake::NoRequest,
        }
    }
}

impl Drop for HeldDispatch {
    fn drop(&mut self) {
        let _released = self.live.lock().held.remove(&self.ticket);
    }
}

/// How the watchdog asks the worker to stop after it failed closed, and
/// how the worker reads why.
#[derive(Clone)]
pub struct FailClosedStop {
    /// Cancelled to ask the worker to stop.
    worker_stop: CancellationToken,
    cause: Arc<OnceLock<String>>,
}

/// Whether the runner heartbeats failed closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailClosedState {
    Watching,
    /// The watchdog failed closed for this cause and asked for the stop.
    FailedClosed {
        cause: String,
    },
}

impl FailClosedStop {
    /// A stop that cancels `worker_stop`, the worker's own stop token.
    pub fn new(worker_stop: CancellationToken) -> Self {
        Self {
            worker_stop,
            cause: Arc::new(OnceLock::new()),
        }
    }

    fn request(&self, cause: String) {
        // Only the watchdog sets the cause, once; a second set is ignored.
        let _first = self.cause.set(cause);
        self.worker_stop.cancel();
    }

    /// Whether the watchdog failed closed, and why. The worker reads it
    /// after its stop, to end with an error.
    pub fn state(&self) -> FailClosedState {
        match self.cause.get() {
            Some(cause) => FailClosedState::FailedClosed {
                cause: cause.clone(),
            },
            None => FailClosedState::Watching,
        }
    }
}

/// Whether a reaper of this config fails RUNNING rows whose beats stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleRunningRecovery {
    /// The reaper fails a RUNNING row with no runner beat for `threshold`;
    /// the worker fails closed when no beat lands for half of it.
    On { threshold: Duration },
    /// No reaper fails a RUNNING row: a beat that cannot land puts no running
    /// task at risk, so the worker logs it and runs on.
    Off,
}

impl StaleRunningRecovery {
    /// Read the rule from the worker's recovery config.
    pub fn from_config(recovery: &RecoveryConfig) -> Self {
        match recovery.auto_fail_stale_running {
            true => Self::On {
                threshold: Duration::from_millis(recovery.running_stale_threshold_ms),
            },
            false => Self::Off,
        }
    }
}

/// Half of the stale threshold: the time with no landed beat after which the
/// worker fails closed.
fn fail_closed_window(threshold: Duration) -> Duration {
    threshold / FAIL_CLOSED_WINDOW_DIVISOR as u32
}

/// Where the fail-closed window of a landed beat starts: at the landing,
/// except for a beat whose write took longer than the window less the kill
/// margin. Its window starts earlier, so it ends [`FAIL_CLOSED_KILL_MARGIN`]
/// before the threshold counted from `started_at`. The reaper counts the beat
/// from its `sent_at`, which is not earlier than the start of the write.
fn window_start(threshold: Duration, started_at: Instant, landed_at: Instant) -> Instant {
    let latest = started_at
        + threshold.saturating_sub(FAIL_CLOSED_KILL_MARGIN + fail_closed_window(threshold));
    landed_at.min(latest)
}

/// The retry wait after `consecutive_failures` failed writes.
fn retry_delay(consecutive_failures: u32, interval: Duration) -> Duration {
    let doublings = consecutive_failures
        .saturating_sub(1)
        .min(BEAT_RETRY_DOUBLINGS_MAX);
    (BEAT_RETRY_FIRST_DELAY * 2u32.pow(doublings))
        .min(BEAT_RETRY_MAX_DELAY)
        .min(interval)
}

/// The last write that failed, or none since the last landed beat.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LastFailure {
    NoFailure,
    WriteFailed(String),
}

/// What the writer last achieved, shared with the watchdog thread.
#[derive(Debug, Clone)]
struct Coverage {
    /// From when the watchdog counts: the window start of the last landed
    /// beat, or the time of a pass that held no dispatch.
    covered_at: Instant,
    consecutive_failures: u32,
    last_failure: LastFailure,
}

#[derive(Clone)]
struct SharedCoverage(Arc<Mutex<Coverage>>);

impl SharedCoverage {
    fn new(now: Instant) -> Self {
        Self(Arc::new(Mutex::new(Coverage {
            covered_at: now,
            consecutive_failures: 0,
            last_failure: LastFailure::NoFailure,
        })))
    }

    fn read(&self) -> Coverage {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Record a covered pass; returns the failures it ended.
    fn record_covered(&self, covered_at: Instant) -> u32 {
        let mut coverage = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let ended = coverage.consecutive_failures;
        *coverage = Coverage {
            covered_at,
            consecutive_failures: 0,
            last_failure: LastFailure::NoFailure,
        };
        ended
    }

    /// Record a failed write; returns the failures in a row.
    fn record_failure(&self, detail: String) -> u32 {
        let mut coverage = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        coverage.consecutive_failures = coverage.consecutive_failures.saturating_add(1);
        coverage.last_failure = LastFailure::WriteFailed(detail);
        coverage.consecutive_failures
    }
}

/// Who writes the beats: the worker's identity on each heartbeat row.
#[derive(Debug, Clone)]
pub struct BeatSender {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
}

/// The outcome of one runner-beat statement.
#[derive(Debug)]
enum BeatWrite {
    /// The statement ran; these runs passed the fence and got their beat.
    Landed(Vec<RunClaim>),
    /// The statement ran, and a returned row did not decode: the beats
    /// landed, and which runs they covered is not known.
    LandedNotRead,
}

/// Write one runner-beat statement for the given dispatches, in autocommit
/// on `pool`. It does not retry.
///
/// Errors: the failed checkout or statement; nothing was written, or the
/// outcome is unknown.
async fn write_beats(
    pool: &CorePool,
    sender: &BeatSender,
    task_ids: &[Uuid],
    claim_generations: &[DateTime<Utc>],
) -> Result<BeatWrite, StatementFailure> {
    let bound = INSERT_RUNNER_HEARTBEATS
        .bind()
        .value(&sender.worker_id)
        .value(&sender.hostname)
        .value(&sender.pid)
        .value(&task_ids)
        .value(&claim_generations)
        .finish();
    let rows = pool
        .query(
            CheckoutClass::Reserved,
            &bound,
            StatementBound::ConnectionTimeouts,
        )
        .await?;
    let landed: Option<Vec<RunClaim>> = rows.iter().map(landed_run).collect();
    Ok(match landed {
        Some(landed) => BeatWrite::Landed(landed),
        None => BeatWrite::LandedNotRead,
    })
}

/// The run of one row of the beat statement, or `None` when a column does
/// not decode (logged).
fn landed_run(row: &Row) -> Option<RunClaim> {
    let statement = &INSERT_RUNNER_HEARTBEATS;
    let read = column::<Uuid>(row, statement, "task_id").and_then(|task_id| {
        column::<DateTime<Utc>>(row, statement, "claimed_at").map(|claimed_at| RunClaim {
            task_id,
            claimed_at,
        })
    });
    match read {
        Ok(run) => Some(run),
        Err(error) => {
            tracing::error!(
                %error,
                "a row of the runner heartbeat statement did not decode: {error}",
            );
            None
        }
    }
}

/// The pool of the runner beats: one connection of its own
/// ([`PoolRole::RunnerHeartbeat`]) on the `database_url` of `config`,
/// outside the worker profile pool, so a busy pool cannot starve the beats
/// (Python `PostgresBeatWriter`: one psycopg connection in autocommit, on
/// the worker's DSN with the TCP settings of the config). The connection
/// opens at the first write and again after it was lost; it is never
/// pinged and never replaced for its age. Each connect is bounded by
/// [`BEAT_CONNECT_TIMEOUT`]. Behind PgBouncer transaction pooling it sends
/// unnamed statements only (Python `prepare_threshold=None`).
///
/// Reads the process connection environment (`PGPASSWORD`, `~/.pgpass`, the
/// TLS files the URL names). Connects to nothing.
///
/// Errors: [`PoolSettingsError`] when the environment, the URL or its
/// connection plan is refused; nothing was built.
pub fn runner_beat_pool(config: &PostgresConfig) -> Result<CorePool, PoolSettingsError> {
    let environment = ConnectionEnvironment::capture().map_err(PoolSettingsError::Environment)?;
    let protocol = match config.pgbouncer_transaction_mode {
        true => StatementProtocol::UnnamedOnly,
        false => StatementProtocol::PreparedPerConnection,
    };
    runner_beat_pool_on(
        &config.database_url,
        &config.socket_settings(),
        protocol,
        &environment,
    )
}

/// [`runner_beat_pool`] on `url` with the given socket settings, statement
/// protocol and connection environment.
///
/// Errors: [`PoolSettingsError::Url`] or [`PoolSettingsError::Plan`].
pub(crate) fn runner_beat_pool_on(
    url: &str,
    socket: &SocketSettings,
    protocol: StatementProtocol,
    environment: &ConnectionEnvironment,
) -> Result<CorePool, PoolSettingsError> {
    let role = PoolRole::RunnerHeartbeat;
    let settings = parse_connection_url(url, environment)
        .map_err(|cause| PoolSettingsError::Url { role, cause })?
        .with_connect_timeout(BEAT_CONNECT_TIMEOUT);
    let connect = ConnectPlan::load(&settings, environment, socket)
        .map_err(|cause| PoolSettingsError::Plan { role, cause })?;
    let plan = PoolPlan {
        role,
        sizing: PoolSizing::new(1, Reserve::NoReserve)
            .expect("one connection with no reserve is a valid size"),
        checkout_wait: BEAT_CONNECT_TIMEOUT,
        idle_ping: IdlePing::Off,
        max_age: ConnectionAge::Unlimited,
        protocol,
    };
    Ok(CorePool::new(plan, Arc::new(connect)))
}

/// A beat pool on the test database `url`, with the default socket
/// settings and an isolated environment (test URLs carry their password).
#[cfg(test)]
pub(crate) fn test_beat_pool(url: &str) -> Arc<CorePool> {
    Arc::new(
        runner_beat_pool_on(
            url,
            &crate::db::DEFAULT_SOCKET_SETTINGS,
            StatementProtocol::PreparedPerConnection,
            &ConnectionEnvironment::isolated(),
        )
        .expect("the statement layer accepts a test URL"),
    )
}

/// The longest wait of [`end_child`] for the exit of a test child, and for
/// the stop of its spawner.
#[cfg(test)]
const TEST_CHILD_END_WAIT: Duration = Duration::from_secs(10);

/// A child of `spawner` that sleeps: what a held dispatch of a test runs
/// on. End it with [`end_child`].
#[cfg(test)]
pub(crate) async fn sleeping_child(
    spawner: &crate::worker::executor::supervisor::Spawner,
) -> (crate::worker::executor::supervisor::SpawnedChild, HeldChild) {
    let spawned = spawner
        .handle()
        .spawn(crate::worker::executor::supervisor::ChildCommand {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "exec sleep 60".into()],
        })
        .await
        .expect("spawn a sleeping child");
    let child = HeldChild {
        id: spawned.id,
        pid: spawned.pid,
        spawner: spawner.handle(),
        kill_mark: WorkerKillMark::default(),
        cancel_bell: CancelBell::default(),
    };
    (spawned, child)
}

/// Kill the child of [`sleeping_child`] if it still runs, then stop the
/// spawner.
#[cfg(test)]
pub(crate) async fn end_child(
    spawner: crate::worker::executor::supervisor::Spawner,
    mut spawned: crate::worker::executor::supervisor::SpawnedChild,
) {
    let _outcome = spawner.handle().kill_session(spawned.id).await;
    let _exit =
        crate::worker::executor::supervisor::wait_for_exit(&mut spawned.exit, TEST_CHILD_END_WAIT)
            .await;
    let _report = spawner.stop(TEST_CHILD_END_WAIT);
}

/// The watchdog thread of the runner beats. Dropping it does not stop the
/// thread; call [`RunnerBeatWatch::stop`].
pub struct RunnerBeatWatch {
    stop_watch: Sender<()>,
    watch: std::thread::JoinHandle<()>,
}

/// What the watchdog acts on: the registry it closes and the stop it asks
/// for.
struct WatchContext {
    stale_running: StaleRunningRecovery,
    interval: Duration,
    coverage: SharedCoverage,
    dead_writer: DeadWriter,
    live: LiveDispatches,
    fail_closed_stop: FailClosedStop,
}

/// Why the beat writer ended with no stop request, recorded once for the
/// watchdog. Only the writer of stale running recovery off records it: with
/// recovery on, the worker's supervision stops the worker.
#[derive(Clone, Default)]
struct DeadWriter(Arc<OnceLock<String>>);

impl DeadWriter {
    /// Record why the writer ended; a later record is ignored.
    fn record(&self, reason: String) {
        let _first = self.0.set(reason);
    }

    /// Why the writer ended, once it ended with no stop request.
    fn reason(&self) -> Option<String> {
        self.0.get().cloned()
    }
}

/// Aborts the writer task when the task that awaits it ends or is dropped.
struct AbortWriterOnDrop(tokio::task::AbortHandle);

impl Drop for AbortWriterOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Start the beat writer on the current runtime and its watchdog on its own
/// thread. Returns the watchdog and the task that the worker supervises with
/// its other service loops. The writer stops when `cancel` fires; the
/// watchdog when [`RunnerBeatWatch::stop`] runs. A watchdog that fails
/// closed asks for the stop through `fail_closed_stop`.
///
/// The supervised task, per `stale_running`:
/// - on: the writer itself. A writer that ends with no stop request ends the
///   task, and the worker's supervision stops the worker (fail-fatal).
/// - off: a task that awaits the writer (Python: the watchdog logs a dead
///   beat thread and the worker runs on). A writer that ends with no stop
///   request is logged once by the watchdog, which then ends; the task waits
///   for `cancel`, so the supervision sees no early end. An abort of the task
///   aborts the writer.
///
/// The writer closes `pool` when it ends.
///
/// Errors: the OS refused to start the watchdog thread; the writer is not
/// started then.
pub fn launch_runner_beats(
    pool: Arc<CorePool>,
    sender: BeatSender,
    interval: Duration,
    stale_running: StaleRunningRecovery,
    live: LiveDispatches,
    cancel: CancellationToken,
    fail_closed_stop: FailClosedStop,
) -> std::io::Result<(RunnerBeatWatch, JoinHandle<()>)> {
    let writer_live = live.clone();
    let writer_cancel = cancel.clone();
    launch_with_writer(
        interval,
        stale_running,
        live,
        cancel,
        fail_closed_stop,
        move |coverage| {
            run_beats(
                pool,
                sender,
                interval,
                stale_running,
                writer_live,
                coverage,
                writer_cancel,
            )
        },
    )
}

/// [`launch_runner_beats`] with the writer that `writer` builds on the
/// coverage it shares with the watchdog.
///
/// Errors: the OS refused to start the watchdog thread; the writer is not
/// started then.
fn launch_with_writer<W>(
    interval: Duration,
    stale_running: StaleRunningRecovery,
    live: LiveDispatches,
    cancel: CancellationToken,
    fail_closed_stop: FailClosedStop,
    writer: impl FnOnce(SharedCoverage) -> W,
) -> std::io::Result<(RunnerBeatWatch, JoinHandle<()>)>
where
    W: std::future::Future<Output = ()> + Send + 'static,
{
    let coverage = SharedCoverage::new(Instant::now());
    let dead_writer = DeadWriter::default();
    let (stop_watch, stop_received) = std::sync::mpsc::channel::<()>();
    let context = WatchContext {
        stale_running,
        interval,
        coverage: coverage.clone(),
        dead_writer: dead_writer.clone(),
        live,
        fail_closed_stop,
    };
    // The watchdog thread and the writer log to the dispatcher of their
    // starter.
    let watch = std::thread::Builder::new()
        .name(WATCH_THREAD_NAME.to_owned())
        .spawn(with_starter_dispatch(move || {
            run_watch(&context, &stop_received);
        }))?;
    let writer = tokio::spawn(writer(coverage).with_current_subscriber());
    let supervised = match stale_running {
        StaleRunningRecovery::On { .. } => writer,
        StaleRunningRecovery::Off => tokio::spawn(
            run_on_after_writer_end(writer, dead_writer, cancel).with_current_subscriber(),
        ),
    };
    Ok((RunnerBeatWatch { stop_watch, watch }, supervised))
}

/// With stale running recovery off, the task that the worker supervises in
/// place of the writer: await the writer; when it ended with no stop request
/// (`cancel` not fired), record why for the watchdog and wait for `cancel`.
/// Dropping this task aborts the writer.
async fn run_on_after_writer_end(
    writer: JoinHandle<()>,
    dead_writer: DeadWriter,
    cancel: CancellationToken,
) {
    let _abort_writer = AbortWriterOnDrop(writer.abort_handle());
    let ended = writer.await;
    match cancel.is_cancelled() {
        true => {}
        false => {
            dead_writer.record(crate::worker::supervision::unexpected_end_reason(ended));
            cancel.cancelled().await;
        }
    }
}

impl RunnerBeatWatch {
    /// Stop the watchdog and wait for its thread. Call it after every
    /// dispatch has ended: from then on no held task needs a beat.
    pub fn stop(self) {
        // A send error means the watchdog already ended.
        let _ignored = self.stop_watch.send(());
        if self.watch.join().is_err() {
            tracing::error!("the runner heartbeat watchdog thread panicked");
        }
    }
}

/// The writer loop: wait one interval, then beat; a failed write is retried
/// sooner. The start writes no beat: a task gets its first beat at the first
/// pass after its start, and until then the stale RUNNING rule judges it
/// from its `started_at`.
async fn run_beats(
    pool: Arc<CorePool>,
    sender: BeatSender,
    interval: Duration,
    stale_running: StaleRunningRecovery,
    live: LiveDispatches,
    coverage: SharedCoverage,
    cancel: CancellationToken,
) {
    let mut failure_log = LogGate::new(BEAT_FAILURE_LOG_INTERVAL);
    let mut delay = interval;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(delay) => {}
        }
        delay = beat_once(
            &pool,
            &sender,
            interval,
            stale_running,
            &live,
            &coverage,
            &mut failure_log,
        )
        .await;
    }
    pool.close();
}

/// One beat pass; returns the wait before the next pass.
async fn beat_once(
    pool: &CorePool,
    sender: &BeatSender,
    interval: Duration,
    stale_running: StaleRunningRecovery,
    live: &LiveDispatches,
    coverage: &SharedCoverage,
    failure_log: &mut LogGate,
) -> Duration {
    let started_at = Instant::now();
    let (task_ids, claim_generations) = live.snapshot();
    if task_ids.is_empty() {
        // No held task, so none is at risk.
        let _ended = coverage.record_covered(started_at);
        return interval;
    }
    match write_beats(pool, sender, &task_ids, &claim_generations).await {
        Ok(written) => {
            let landed_at = Instant::now();
            match written {
                BeatWrite::Landed(landed) => {
                    let sent: Vec<RunClaim> = task_ids
                        .iter()
                        .zip(&claim_generations)
                        .map(|(task_id, claimed_at)| RunClaim {
                            task_id: *task_id,
                            claimed_at: *claimed_at,
                        })
                        .collect();
                    live.note_beat_misses(&sent, &landed);
                }
                BeatWrite::LandedNotRead => {}
            }
            let counts_from = match stale_running {
                StaleRunningRecovery::On { threshold } => {
                    window_start(threshold, started_at, landed_at)
                }
                StaleRunningRecovery::Off => landed_at,
            };
            let ended = coverage.record_covered(counts_from);
            if ended > 0 {
                tracing::info!(failed_attempts = ended, "runner heartbeat write recovered");
                failure_log.reset();
            }
            (started_at + interval).saturating_duration_since(landed_at)
        }
        Err(error) => {
            let failures = coverage.record_failure(error.to_string());
            if failures == 1 {
                failure_log.reset();
            }
            let now = Instant::now();
            if failure_log.due(now) {
                let uncovered_s = now
                    .saturating_duration_since(coverage.read().covered_at)
                    .as_secs_f64();
                let consequence = match stale_running {
                    StaleRunningRecovery::On { threshold } => format!(
                        "the worker stops at {:.1}s without a landed beat",
                        fail_closed_window(threshold).as_secs_f64()
                    ),
                    StaleRunningRecovery::Off => {
                        "no reaper fails a RUNNING row, so the worker runs on".to_owned()
                    }
                };
                tracing::error!(
                    attempt = failures,
                    held_tasks = task_ids.len(),
                    %error,
                    "Runner heartbeat write failed (attempt {failures}, {uncovered_s:.1}s since \
                     the last beat; {consequence}): {error}",
                );
            }
            retry_delay(failures, interval)
        }
    }
}

/// Logs at most once per interval; a reset allows the next log at once.
struct LogGate {
    interval: Duration,
    last: Option<Instant>,
}

impl LogGate {
    fn new(interval: Duration) -> Self {
        Self {
            interval,
            last: None,
        }
    }

    fn due(&mut self, now: Instant) -> bool {
        match self.last {
            Some(at) if now.saturating_duration_since(at) < self.interval => false,
            Some(_) | None => {
                self.last = Some(now);
                true
            }
        }
    }

    fn reset(&mut self) {
        self.last = None;
    }
}

/// What one watchdog check found.
#[derive(Debug, Clone, PartialEq)]
enum WatchVerdict {
    /// A beat landed within the window.
    Current,
    /// No beat landed for `uncovered`, past `window`.
    Overdue {
        uncovered: Duration,
        window: Duration,
        last_failure: LastFailure,
    },
}

/// Compare the time since the last covered pass with `window`.
fn check_coverage(coverage: &Coverage, window: Duration, now: Instant) -> WatchVerdict {
    let uncovered = now.saturating_duration_since(coverage.covered_at);
    match uncovered > window {
        true => WatchVerdict::Overdue {
            uncovered,
            window,
            last_failure: coverage.last_failure.clone(),
        },
        false => WatchVerdict::Current,
    }
}

/// How a watch with stale running recovery on ended.
#[derive(Debug, Clone, PartialEq)]
enum WatchEnd {
    /// The worker asked the watchdog to stop.
    Stopped,
    /// No beat landed within the fail-closed window: the process must end.
    FailClosed(WatchVerdict),
}

/// Check every [`WATCH_INTERVAL`] until a stop or an overdue beat.
fn watch_until_end(window: Duration, coverage: &SharedCoverage, stop: &Receiver<()>) -> WatchEnd {
    loop {
        match stop.recv_timeout(WATCH_INTERVAL) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return WatchEnd::Stopped,
            Err(RecvTimeoutError::Timeout) => {}
        }
        match check_coverage(&coverage.read(), window, Instant::now()) {
            WatchVerdict::Current => {}
            overdue @ WatchVerdict::Overdue { .. } => return WatchEnd::FailClosed(overdue),
        }
    }
}

/// The watchdog thread body.
fn run_watch(context: &WatchContext, stop: &Receiver<()>) {
    match context.stale_running {
        StaleRunningRecovery::On { threshold } => {
            let window = fail_closed_window(threshold);
            match watch_until_end(window, &context.coverage, stop) {
                WatchEnd::Stopped => {}
                WatchEnd::FailClosed(verdict) => fail_closed(context, &verdict, stop),
            }
        }
        StaleRunningRecovery::Off => watch_and_log(
            context.interval * LATE_BEAT_INTERVALS,
            &context.coverage,
            &context.dead_writer,
            stop,
        ),
    }
}

/// No runner beat landed within the fail-closed window, so the reaper may
/// fail rows whose attempts still run here (Python `_fail_closed`). Close
/// the registry, kill every child that held a task, ask the worker to stop,
/// and exit when it has not stopped within [`FAIL_CLOSED_EXIT_GRACE`]. A
/// panic here also ends the process.
fn fail_closed(context: &WatchContext, verdict: &WatchVerdict, stop: &Receiver<()>) {
    let cause = describe(verdict);
    tracing::error!(
        cause = %cause,
        "worker failed closed: runner heartbeats stopped; every child that holds a task \
         is killed now, so the reaper cannot fail a task that still runs",
    );
    let stopped = std::panic::catch_unwind(AssertUnwindSafe(|| {
        kill_held_children(&context.live);
        context.fail_closed_stop.request(cause);
        match stop.recv_timeout(FAIL_CLOSED_EXIT_GRACE) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => WorkerStop::Stopped,
            Err(RecvTimeoutError::Timeout) => {
                tracing::error!(
                    exit_code = RUNNER_HEARTBEAT_LOST_EXIT_CODE,
                    grace_s = FAIL_CLOSED_EXIT_GRACE.as_secs(),
                    "the worker did not stop within the grace; ending the process",
                );
                WorkerStop::NotStopped
            }
        }
    }));
    match stopped {
        Ok(WorkerStop::Stopped) => {}
        Ok(WorkerStop::NotStopped) => std::process::exit(RUNNER_HEARTBEAT_LOST_EXIT_CODE),
        Err(_) => {
            tracing::error!(
                exit_code = RUNNER_HEARTBEAT_LOST_EXIT_CODE,
                "failing closed panicked; ending the process",
            );
            std::process::exit(RUNNER_HEARTBEAT_LOST_EXIT_CODE);
        }
    }
}

/// Whether the worker stopped within the grace after a fail-closed request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerStop {
    Stopped,
    NotStopped,
}

/// The children of a closed registry, each once, with the tasks it held.
struct ChildToKill {
    child: HeldChild,
    task_ids: Vec<Uuid>,
}

/// Close the registry, mark each child that held a task as killed by the
/// worker, and queue `SIGKILL` to its session through the spawner thread.
/// The answers are read for the log, at most
/// [`FAIL_CLOSED_KILL_ANSWER_WAIT`] in all. The fail-closed exit of the
/// worker liveness lock ([`crate::worker::liveness`]) calls it too.
pub(crate) fn kill_held_children(live: &LiveDispatches) {
    let mut children: BTreeMap<ChildId, ChildToKill> = BTreeMap::new();
    for LiveDispatch { task_id, child, .. } in live.close() {
        children
            .entry(child.id)
            .or_insert_with(|| ChildToKill {
                child,
                task_ids: Vec::new(),
            })
            .task_ids
            .push(task_id);
    }
    let mut pending: Vec<(ChildToKill, oneshot::Receiver<KillOutcome>)> = Vec::new();
    for (_, to_kill) in children {
        to_kill.child.kill_mark.mark();
        match to_kill.child.spawner.request_kill(to_kill.child.id) {
            Ok(answer) => pending.push((to_kill, answer)),
            Err(refused) => log_kill(&to_kill, refused),
        }
    }
    read_kill_answers(pending);
}

/// Read the spawner's answers until each came or the wait passed.
fn read_kill_answers(mut pending: Vec<(ChildToKill, oneshot::Receiver<KillOutcome>)>) {
    let until = Instant::now() + FAIL_CLOSED_KILL_ANSWER_WAIT;
    loop {
        let mut waiting = Vec::with_capacity(pending.len());
        for (to_kill, mut answer) in pending {
            match answer.try_recv() {
                Ok(outcome) => log_kill(&to_kill, outcome),
                Err(oneshot::error::TryRecvError::Closed) => {
                    log_kill(&to_kill, KillOutcome::SpawnerStopped);
                }
                Err(oneshot::error::TryRecvError::Empty) => waiting.push((to_kill, answer)),
            }
        }
        pending = waiting;
        if pending.is_empty() {
            return;
        }
        if Instant::now() >= until {
            for (to_kill, _) in &pending {
                tracing::error!(
                    pid = to_kill.child.pid.get(),
                    tasks = ?to_kill.task_ids,
                    "the spawner did not answer the kill of a child that held tasks in time",
                );
            }
            return;
        }
        std::thread::sleep(KILL_ANSWER_POLL);
    }
}

fn log_kill(to_kill: &ChildToKill, outcome: KillOutcome) {
    let pid = to_kill.child.pid.get();
    match outcome {
        KillOutcome::Sent => tracing::error!(
            pid,
            tasks = ?to_kill.task_ids,
            "killed a child that held tasks",
        ),
        KillOutcome::SentToReapedSession => tracing::error!(
            pid,
            tasks = ?to_kill.task_ids,
            "killed the processes left in the session of a reaped child that held tasks",
        ),
        KillOutcome::SessionGone | KillOutcome::PidReused => {}
        KillOutcome::NotPermitted
        | KillOutcome::Failed { .. }
        | KillOutcome::QueueFull
        | KillOutcome::SpawnerStopped => tracing::error!(
            pid,
            tasks = ?to_kill.task_ids,
            outcome = ?outcome,
            "cannot kill a child that held tasks",
        ),
    }
}

fn describe(verdict: &WatchVerdict) -> String {
    match verdict {
        WatchVerdict::Current => "runner heartbeats are current".to_owned(),
        WatchVerdict::Overdue {
            uncovered,
            window,
            last_failure,
        } => {
            let last = match last_failure {
                LastFailure::NoFailure => "the last write has not returned".to_owned(),
                LastFailure::WriteFailed(detail) => format!("last error: {detail}"),
            };
            format!(
                "no runner heartbeat landed for {:.1}s, past {:.1}s (half of \
                 running_stale_threshold_ms); {last}",
                uncovered.as_secs_f64(),
                window.as_secs_f64(),
            )
        }
    }
}

/// With stale running recovery off: log late beats at most once per log
/// interval, never stop the worker. A writer that died is logged once, and
/// the watch ends: no beat lands any more, and no task depends on one
/// (Python `_watch_and_log`).
fn watch_and_log(
    late_after: Duration,
    coverage: &SharedCoverage,
    dead_writer: &DeadWriter,
    stop: &Receiver<()>,
) {
    let mut late_log = LogGate::new(BEAT_FAILURE_LOG_INTERVAL);
    loop {
        match stop.recv_timeout(WATCH_INTERVAL) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {}
        }
        if let Some(reason) = dead_writer.reason() {
            tracing::error!(
                cause = %reason,
                "{DEAD_WRITER_LINE}: {reason}. The worker runs on without runner heartbeats, \
                 because no reaper fails a RUNNING row (auto_fail_stale_running is off, or no \
                 recovery config)",
            );
            return;
        }
        let now = Instant::now();
        match check_coverage(&coverage.read(), late_after, now) {
            WatchVerdict::Current => late_log.reset(),
            verdict @ WatchVerdict::Overdue { .. } => {
                if late_log.due(now) {
                    tracing::warn!(
                        cause = %describe(&verdict),
                        "runner heartbeats are late; the worker runs on, because no \
                         reaper fails a RUNNING row (auto_fail_stale_running is off)",
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod fail_closed_tests;

#[cfg(test)]
mod tests {

    use super::*;
    use crate::test_support::sql::{self, TestPool};
    use crate::worker::executor::supervisor::Spawner;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn held_dispatches_are_beaten_until_the_guard_drops() {
        let spawner = Spawner::start().unwrap();
        let (spawned, child) = sleeping_child(&spawner).await;
        let live = LiveDispatches::default();
        let task_id = Uuid::new_v4();
        let claimed_at = Utc::now();
        let held = live.hold(task_id, claimed_at, child).unwrap();
        assert_eq!(live.snapshot(), (vec![task_id], vec![claimed_at]));
        drop(held);
        assert_eq!(live.snapshot(), (Vec::new(), Vec::new()));
        end_child(spawner, spawned).await;
    }

    /// Tickets: the guard of an ended dispatch does not end the beats of a
    /// later dispatch of the same task.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_old_guard_does_not_release_a_later_hold_of_the_same_task() {
        let spawner = Spawner::start().unwrap();
        let (spawned, child) = sleeping_child(&spawner).await;
        let live = LiveDispatches::default();
        let task_id = Uuid::new_v4();
        let first = Utc::now();
        let second = first + chrono::Duration::seconds(1);
        let old = live.hold(task_id, first, child.clone()).unwrap();
        let new = live.hold(task_id, second, child).unwrap();
        drop(old);
        assert_eq!(live.snapshot(), (vec![task_id], vec![second]));
        drop(new);
        assert_eq!(live.snapshot(), (Vec::new(), Vec::new()));
        end_child(spawner, spawned).await;
    }

    /// `close` refuses later holds and
    /// returns what is held, in ticket order; a second close returns
    /// nothing, and a guard dropped after the close changes nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_closed_registry_refuses_holds_and_gives_up_what_it_held() {
        let spawner = Spawner::start().unwrap();
        let (spawned, child) = sleeping_child(&spawner).await;
        let live = LiveDispatches::default();
        let ids = [Uuid::new_v4(), Uuid::new_v4()];
        let claimed_at = Utc::now();
        let guards: Vec<HeldDispatch> = ids
            .iter()
            .map(|id| live.hold(*id, claimed_at, child.clone()).unwrap())
            .collect();
        let closed: Vec<Uuid> = live.close().iter().map(|held| held.task_id).collect();
        assert_eq!(closed, ids.to_vec());
        assert_eq!(live.snapshot(), (Vec::new(), Vec::new()));
        assert!(matches!(
            live.hold(Uuid::new_v4(), claimed_at, child),
            Err(RegistryClosed)
        ));
        drop(guards);
        assert!(live.close().is_empty());
        end_child(spawner, spawned).await;
    }

    /// The kill of held children: the watchdog marks each held child as
    /// killed by the worker and kills its session through the spawner thread.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_fail_closed_stop_marks_and_kills_each_held_child() {
        use crate::worker::executor::supervisor::{
            wait_for_exit, ChildCommand, ChildExit, ExitState, ReapedExit,
        };
        let spawner = Spawner::start().unwrap();
        let mut spawned = spawner
            .handle()
            .spawn(ChildCommand {
                program: "/bin/sh".into(),
                args: vec!["-c".into(), "exec sleep 60".into()],
            })
            .await
            .unwrap();
        let kill_mark = WorkerKillMark::default();
        let child = HeldChild {
            id: spawned.id,
            pid: spawned.pid,
            spawner: spawner.handle(),
            kill_mark: kill_mark.clone(),
            cancel_bell: CancelBell::default(),
        };
        let live = LiveDispatches::default();
        let claimed_at = Utc::now();
        let _first = live
            .hold(Uuid::new_v4(), claimed_at, child.clone())
            .unwrap();
        let _second = live
            .hold(Uuid::new_v4(), claimed_at, child.clone())
            .unwrap();
        let closing = live.clone();
        tokio::task::spawn_blocking(move || kill_held_children(&closing))
            .await
            .unwrap();
        assert_eq!(
            wait_for_exit(&mut spawned.exit, Duration::from_secs(10)).await,
            ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGKILL))
        );
        assert_ne!(*spawned.exit.borrow(), ExitState::Running);
        assert!(matches!(
            live.hold(Uuid::new_v4(), claimed_at, child),
            Err(RegistryClosed)
        ));
        assert_eq!(
            kill_mark.read(),
            crate::worker::executor::supervisor::channel::KillMark::KilledByParent
        );
        let _ = spawner.stop(Duration::from_secs(10));
    }

    #[test]
    fn the_fail_closed_stop_cancels_the_worker_and_keeps_its_cause() {
        let worker_stop = CancellationToken::new();
        let stop = FailClosedStop::new(worker_stop.clone());
        assert_eq!(stop.state(), FailClosedState::Watching);
        stop.request("no runner heartbeat landed".to_owned());
        assert!(worker_stop.is_cancelled());
        assert_eq!(
            stop.state(),
            FailClosedState::FailedClosed {
                cause: "no runner heartbeat landed".to_owned()
            }
        );
    }

    /// The fail-closed window is half the
    /// threshold, and it holds one interval plus the landing allowance at the
    /// smallest threshold the config accepts.
    #[test]
    fn the_fail_closed_window_is_half_the_threshold() {
        let recovery = RecoveryConfig {
            runner_heartbeat_interval_ms: 1_000,
            running_stale_threshold_ms:
                crate::core::config::recovery::fail_closed_threshold_floor_ms(1_000),
            ..RecoveryConfig::default()
        };
        let StaleRunningRecovery::On { threshold } = StaleRunningRecovery::from_config(&recovery)
        else {
            panic!("auto_fail_stale_running is on by default");
        };
        assert_eq!(
            fail_closed_window(threshold),
            Duration::from_millis(
                1_000 + crate::core::config::recovery::RUNNER_BEAT_LANDING_ALLOWANCE_MS
            ),
        );

        let off = RecoveryConfig {
            auto_fail_stale_running: false,
            ..recovery
        };
        assert_eq!(
            StaleRunningRecovery::from_config(&off),
            StaleRunningRecovery::Off
        );
    }

    /// A slow write moves its window start earlier, so the window still ends
    /// the kill margin before the threshold counted from the write's start.
    #[test]
    fn a_slow_beat_starts_its_window_early() {
        let threshold = Duration::from_secs(64);
        let started_at = Instant::now();
        let quick = started_at + Duration::from_millis(100);
        assert_eq!(window_start(threshold, started_at, quick), quick);

        let slow = started_at + Duration::from_secs(40);
        let start = window_start(threshold, started_at, slow);
        assert!(start < slow);
        assert_eq!(
            start + fail_closed_window(threshold),
            started_at + threshold - FAIL_CLOSED_KILL_MARGIN,
        );
    }

    #[test]
    fn retry_delay_doubles_up_to_its_bounds() {
        let interval = Duration::from_secs(30);
        assert_eq!(retry_delay(1, interval), Duration::from_millis(250));
        assert_eq!(retry_delay(2, interval), Duration::from_millis(500));
        assert_eq!(retry_delay(5, interval), Duration::from_secs(4));
        assert_eq!(retry_delay(6, interval), BEAT_RETRY_MAX_DELAY);
        assert_eq!(retry_delay(u32::MAX, interval), BEAT_RETRY_MAX_DELAY);
        assert_eq!(
            retry_delay(6, Duration::from_secs(1)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn a_covered_pass_is_current_and_a_late_one_is_overdue() {
        let now = Instant::now();
        let coverage = SharedCoverage::new(now);
        let window = Duration::from_secs(10);
        assert_eq!(
            check_coverage(&coverage.read(), window, now + Duration::from_secs(9)),
            WatchVerdict::Current,
        );

        assert_eq!(coverage.record_failure("connection refused".to_owned()), 1);
        assert_eq!(coverage.record_failure("connection refused".to_owned()), 2);
        let verdict = check_coverage(&coverage.read(), window, now + Duration::from_secs(11));
        assert_eq!(
            verdict,
            WatchVerdict::Overdue {
                uncovered: Duration::from_secs(11),
                window,
                last_failure: LastFailure::WriteFailed("connection refused".to_owned()),
            },
        );
        assert!(describe(&verdict).contains("last error: connection refused"));

        assert_eq!(coverage.record_covered(now + Duration::from_secs(11)), 2);
        assert_eq!(
            check_coverage(&coverage.read(), window, now + Duration::from_secs(12)),
            WatchVerdict::Current,
        );
    }

    /// The watch ends with a fail-closed verdict once the window passes with
    /// no landed beat, and with `Stopped` when the worker stops it first.
    #[test]
    fn the_watch_fails_closed_when_no_beat_lands_within_the_window() {
        let coverage = SharedCoverage::new(Instant::now());
        let (_stop_sender, stop_received) = std::sync::mpsc::channel::<()>();
        let end = watch_until_end(Duration::from_millis(300), &coverage, &stop_received);
        assert!(matches!(
            end,
            WatchEnd::FailClosed(WatchVerdict::Overdue {
                last_failure: LastFailure::NoFailure,
                ..
            })
        ));

        let (stop_sender, stop_received) = std::sync::mpsc::channel::<()>();
        stop_sender.send(()).expect("watch receiver is alive");
        assert_eq!(
            watch_until_end(Duration::from_secs(3_600), &coverage, &stop_received),
            WatchEnd::Stopped,
        );
    }

    #[test]
    fn the_runner_beat_statement_is_fenced() {
        for fence in [
            "t.status = 'RUNNING'",
            "t.claimed_by_worker_id = $1::varchar",
            "t.claimed_at = d.claimed_at",
            "t.finalizing_at IS NULL",
        ] {
            assert!(INSERT_RUNNER_HEARTBEATS_SQL.contains(fence), "{fence}");
        }
    }

    /// A RUNNING row on the baseline schema, owned by `owner` at the claim
    /// generation `claimed_at`, with or without the finalizing marker.
    async fn seed_running_task(
        pool: &TestPool,
        task_id: Uuid,
        owner: &str,
        claimed_at: DateTime<Utc>,
        finalizing: bool,
    ) {
        sql::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options, execution,
                retention_class_key, sent_at, created_at, command_fingerprint_version,
                command_fingerprint, idempotency_key_digest, status, enqueued_at,
                claimed_at, claimed_by_worker_id, claim_expires_at, started_at,
                finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname,
                worker_process_name, retry_count, max_retries, next_retry_at, good_until,
                is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
                rerun_root_task_id
            ) VALUES (
                $1, 'runner_beat_test', 'default', 100, '{}', NULL, 'ISOLATED',
                'standard_30d', NOW(), NOW(), 1,
                decode(repeat('00', 32), 'hex'), NULL, 'RUNNING', NOW(),
                $2, $3, NULL, NOW(),
                CASE WHEN $4 THEN NOW() END, CASE WHEN $4 THEN $3 END, 1, 'host',
                'worker', 0, 0, NULL, NULL,
                FALSE, NOW(), FALSE, NULL,
                NULL
            )",
        )
        .bind(task_id)
        .bind(claimed_at)
        .bind(owner)
        .bind(finalizing)
        .execute(pool)
        .await
        .unwrap();
    }

    /// One statement beats the
    /// held RUNNING rows of this worker at their claim generation, and no row
    /// of another generation, another worker, or with the finalizing marker.
    #[tokio::test]
    async fn one_statement_beats_only_rows_that_pass_the_fence() {
        let pool = crate::test_support::migrated_pool().await;
        // The heartbeat table has no default partition: the current hour
        // needs its leaf, as at worker start.
        crate::core::history::maintenance::coverage::ensure_startup_coverage_in_pool(
            &crate::broker::pools::core_pool_for_test_url(pool.url()),
            crate::broker::core_rows::StatementLog::Off,
            2,
            2,
            &[],
        )
        .await
        .expect("partition coverage");
        let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
        let claimed_at: DateTime<Utc> =
            sql::query_scalar("SELECT date_trunc('milliseconds', now())")
                .fetch_one(&pool)
                .await
                .unwrap();
        // ids[0]: passes. ids[1]: other worker. ids[2]: finalizing.
        // ids[3]: other claim generation.
        for (index, id) in ids.iter().enumerate() {
            let owner = match index {
                1 => "other-worker",
                _ => "beat-worker",
            };
            let generation = match index {
                3 => claimed_at - chrono::Duration::seconds(5),
                _ => claimed_at,
            };
            seed_running_task(&pool, *id, owner, generation, index == 2).await;
        }
        let sender = BeatSender {
            worker_id: "beat-worker".to_owned(),
            hostname: "beat-host".to_owned(),
            pid: 7,
        };
        let generations = vec![claimed_at; ids.len()];
        let core_pool = crate::broker::pools::core_pool_for_test_url(pool.url());
        let written = write_beats(&core_pool, &sender, &ids, &generations)
            .await
            .unwrap();
        assert!(
            matches!(&written, BeatWrite::Landed(landed) if landed.len() == 1),
            "{written:?}"
        );
        let beaten: Vec<Uuid> = sql::query_scalar(
            "SELECT task_id FROM horsies_heartbeats
             WHERE role = 'runner' AND sender_id = 'beat-worker' AND task_id = ANY($1)",
        )
        .bind(&ids)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(beaten, vec![ids[0]]);
        sql::query("DELETE FROM horsies_heartbeats WHERE task_id = ANY($1)")
            .bind(&ids)
            .execute(&pool)
            .await
            .unwrap();
        sql::query("DELETE FROM horsies_tasks WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&pool)
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod beat_writer_tests;

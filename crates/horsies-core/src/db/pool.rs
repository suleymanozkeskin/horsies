//! Core connection pools.
//!
//! - One pool per role ([`PoolRole`]); each role has its own size.
//! - A pool can keep a reserve: batch checkouts ([`CheckoutClass::Batch`])
//!   pass a gate that leaves [`Reserve::KeepFree`] connections for reserved
//!   users ([`CheckoutClass::Reserved`]: the reaper and per-task writes). No
//!   batch user holds a connection between batches.
//! - The idle ping: a connection that waited in the pool for longer than
//!   [`IdlePing::AfterIdle`] since its check-in is pinged before it is handed
//!   out; [`IdlePing::EveryCheckout`] pings every pooled connection. deadpool
//!   records the last checkout, not the check-in, so the checkout guard
//!   ([`PooledConnection`]) writes the check-in time into the pooled
//!   connection when it is dropped, and the manager's recycle reads it.
//! - A pool can keep connections open ([`KeptOpen`]): a task opens them when
//!   the pool is built and opens new ones when the count falls below it.
//! - Every pool refuses PgBouncer statement pooling at its first checkout
//!   (`pooling_check`), the session pool included.
//! - A connection that a failure discarded runs nothing more; a dropped open
//!   transaction discards its connection (the server rolls the transaction
//!   back when the socket closes).
//! - A connection of a pool that keeps its session
//!   ([`StatementProtocol::PreparedPerConnection`]) runs
//!   [`KEPT_SESSION_SETUP_SQL`] once when it opens.

use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use deadpool::managed::{
    BuildError, Manager, Metrics, Object, Pool, PoolConfig, PoolError, QueueMode, RecycleError,
    RecycleResult, TimeoutType, Timeouts, WeakPool,
};
use deadpool::Runtime;
use deadpool_postgres::ClientWrapper;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;
use tokio_postgres::error::SqlState;
use tokio_postgres::{CancelToken, Row};
use tokio_postgres_rustls::MakeRustlsConnect;

use super::connect::{open_connection, ConnectPlan, DriverConnection};
use super::ddl::PartitionDdl;
use super::failure::{
    error_scope, CancelRequestOutcome, CheckoutFailure, ConnectError, ConnectFailure, ErrorScope,
    NotSentCause, RefusalCause, StatementFailure, UnknownOutcomeCause,
};
use super::pooling_check::{run_pooling_check, PoolingCheckPassed};
use super::statement::{
    execute_count, query_rows, run_command, AttemptError, BoundStatement, SqlScript, Stage,
    StatementBound, StatementProtocol, CANCEL_GRACE, CANCEL_REQUEST_BOUND,
};
use super::transaction::TransactionCommand;
use crate::process_runtime::spawn_on_process_runtime;

/// PostgreSQL's upper limit of `max_connections`; no pool can be larger.
pub const POSTGRES_MAX_BACKENDS: u32 = 262_143;

/// Connections a worker's profile pool keeps free for the reaper and the
/// per-task writes.
pub const WORKER_RESERVED_CONNECTIONS: NonZeroU32 = match NonZeroU32::new(2) {
    Some(count) => count,
    None => panic!("the worker reserve is not zero"),
};

/// Python's idle-ping rule: ping a connection that waited in the pool for
/// longer than this since its check-in.
pub const DEFAULT_IDLE_PING_AFTER: Duration = Duration::from_secs(1);

/// The bound of one idle ping. A ping that does not answer in time discards
/// the connection.
pub const IDLE_PING_BOUND: Duration = Duration::from_secs(5);

/// How often the task of a pool that keeps connections open compares the
/// open count with [`KeptOpen`].
pub const KEPT_OPEN_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// The session setting of every connection that keeps its session (direct
/// and PgBouncer session pooling). The core's statements are short;
/// PostgreSQL's JIT compiles a statement when its cost estimate passes
/// `jit_above_cost`, and the compile time then exceeds the run time by orders
/// of magnitude (a 2 ms partition coverage probe took 0.7 s or more of JIT
/// compile). A PgBouncer transaction-mode connection does not run it: its
/// server sessions are shared, so a session `SET` would reach other clients.
pub const KEPT_SESSION_SETUP_SQL: &str = "SET jit = off";

const PING_SQL: &str = "SELECT 1";

const LOG_TARGET: &str = "horsies.db";

/// What a pool is for. Each role has its own pool and its own size.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PoolRole {
    /// The main URL (`database_url`) of a producer or a handle.
    Main,
    /// The session URL (`session_database_url`): partition maintenance and
    /// the schema install (its state reads, its advisory lock and its
    /// migrations).
    Session,
    /// The worker parent's pool on the main URL: claim loop, batch writers,
    /// and the reserve.
    WorkerProfile,
    /// The fenced runner heartbeat, on its own connection.
    RunnerHeartbeat,
}

impl PoolRole {
    pub fn as_str(self) -> &'static str {
        match self {
            PoolRole::Main => "main",
            PoolRole::Session => "session",
            PoolRole::WorkerProfile => "worker profile",
            PoolRole::RunnerHeartbeat => "runner heartbeat",
        }
    }
}

impl fmt::Display for PoolRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Connections a pool keeps free from batch checkouts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reserve {
    NoReserve,
    KeepFree(NonZeroU32),
}

/// Connections a pool keeps open whether or not a checkout asked for them
/// (psycopg's `min_size`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeptOpen {
    /// A connection opens at a checkout only.
    NoConnections,
    /// The pool opens this many connections when it is built, and opens a
    /// new one when the count falls below it.
    Connections(NonZeroU32),
}

/// A pool size, its reserve, and the connections it keeps open. Only
/// [`PoolSizing::new`] and [`PoolSizing::keeping_open`] build it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolSizing {
    size: NonZeroU32,
    reserve: Reserve,
    kept_open: KeptOpen,
}

/// A pool size that cannot work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PoolSizingError {
    #[error("a pool needs at least one connection")]
    SizeZero,
    #[error("a pool of {size} connections is above PostgreSQL's limit of {limit}")]
    SizeAboveLimit { size: u32, limit: u32 },
    #[error(
        "a reserve of {reserve} connections leaves no connection for batches in a pool of {size}"
    )]
    ReserveNotBelowSize { size: u32, reserve: u32 },
    #[error("a pool of {size} connections cannot keep {kept} connections open")]
    KeptOpenAboveSize { size: u32, kept: u32 },
}

impl PoolSizing {
    /// Checks that the size is 1 to [`POSTGRES_MAX_BACKENDS`] and that the
    /// reserve leaves at least one connection for batches. The pool keeps no
    /// connection open ([`KeptOpen::NoConnections`]).
    pub fn new(size: u32, reserve: Reserve) -> Result<PoolSizing, PoolSizingError> {
        let Some(size) = NonZeroU32::new(size) else {
            return Err(PoolSizingError::SizeZero);
        };
        if size.get() > POSTGRES_MAX_BACKENDS {
            return Err(PoolSizingError::SizeAboveLimit {
                size: size.get(),
                limit: POSTGRES_MAX_BACKENDS,
            });
        }
        let kept_open = KeptOpen::NoConnections;
        match reserve {
            Reserve::NoReserve => Ok(PoolSizing {
                size,
                reserve,
                kept_open,
            }),
            Reserve::KeepFree(kept) if kept.get() < size.get() => Ok(PoolSizing {
                size,
                reserve,
                kept_open,
            }),
            Reserve::KeepFree(kept) => Err(PoolSizingError::ReserveNotBelowSize {
                size: size.get(),
                reserve: kept.get(),
            }),
        }
    }

    /// This sizing with `kept_open` connections kept open.
    ///
    /// Errors: [`PoolSizingError::KeptOpenAboveSize`] when `kept_open` is
    /// above the size (psycopg refuses a `min_size` above `max_size`).
    pub fn keeping_open(self, kept_open: KeptOpen) -> Result<PoolSizing, PoolSizingError> {
        match kept_open {
            KeptOpen::Connections(kept) if kept.get() > self.size.get() => {
                Err(PoolSizingError::KeptOpenAboveSize {
                    size: self.size.get(),
                    kept: kept.get(),
                })
            }
            KeptOpen::NoConnections | KeptOpen::Connections(_) => {
                Ok(PoolSizing { kept_open, ..self })
            }
        }
    }

    /// The connections the pool keeps open.
    pub fn kept_open(&self) -> KeptOpen {
        self.kept_open
    }

    pub fn size(&self) -> u32 {
        self.size.get()
    }

    pub fn reserve(&self) -> Reserve {
        self.reserve
    }

    /// The most connections batch checkouts may hold at once.
    pub fn batch_limit(&self) -> u32 {
        match self.reserve {
            Reserve::NoReserve => self.size.get(),
            Reserve::KeepFree(kept) => self.size.get() - kept.get(),
        }
    }
}

/// The idle ping of a pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdlePing {
    /// No ping: a dead pooled connection fails its next statement.
    Off,
    /// Ping a connection that waited longer than this since its check-in.
    AfterIdle(Duration),
    /// Ping every pooled connection before it is handed out (psycopg's
    /// `ConnectionPool.check_connection` as a pool's `check`). A connection
    /// that the checkout opens is not pinged: it connected just before.
    EveryCheckout,
}

/// When a pool replaces a connection by age (SQLAlchemy's `pool_recycle`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionAge {
    Unlimited,
    ReplaceAfter(Duration),
}

/// Everything a pool needs besides the connection plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolPlan {
    pub role: PoolRole,
    pub sizing: PoolSizing,
    /// The longest a checkout waits for a free connection (gate included).
    pub checkout_wait: Duration,
    pub idle_ping: IdlePing,
    pub max_age: ConnectionAge,
    pub protocol: StatementProtocol,
}

/// Which checkouts the reserve gate applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckoutClass {
    /// Batch work: passes the reserve gate.
    Batch,
    /// Reserved users: never wait on the gate.
    Reserved,
}

/// The lifecycle stage of a pooled connection, as the pool sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectionState {
    /// Created and not yet returned.
    New,
    /// Returned to the pool in a known state at this time.
    ReturnedAt(Instant),
    /// Must not be handed out again; its driver task was aborted.
    Discarded,
}

/// A pooled connection: the driver client with its statement cache, the
/// handle of its driver task, and its state.
pub(crate) struct CoreConnection {
    client: ClientWrapper,
    driver: AbortHandle,
    state: ConnectionState,
}

/// What a checkout does with a pooled connection before it hands it out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HandOut {
    /// Hand it out as it is.
    AsIs,
    /// Ping it first; a failed ping replaces it.
    AfterPing,
    /// Replace it: it was discarded after its last use.
    Replace,
}

/// The step of a pooled connection in `state` under `idle_ping`, at `now`.
/// Pure.
fn hand_out(idle_ping: IdlePing, state: ConnectionState, now: Instant) -> HandOut {
    match (idle_ping, state) {
        (_, ConnectionState::Discarded) => HandOut::Replace,
        (IdlePing::Off, ConnectionState::New | ConnectionState::ReturnedAt(_))
        | (IdlePing::AfterIdle(_), ConnectionState::New) => HandOut::AsIs,
        (IdlePing::AfterIdle(limit), ConnectionState::ReturnedAt(returned_at))
            if now.saturating_duration_since(returned_at) <= limit =>
        {
            HandOut::AsIs
        }
        (IdlePing::AfterIdle(_), ConnectionState::ReturnedAt(_))
        | (IdlePing::EveryCheckout, ConnectionState::New | ConnectionState::ReturnedAt(_)) => {
            HandOut::AfterPing
        }
    }
}

/// Creates and recycles connections for one pool.
pub(crate) struct CoreManager {
    plan: Arc<ConnectPlan>,
    idle_ping: IdlePing,
    max_age: ConnectionAge,
    protocol: StatementProtocol,
}

impl Manager for CoreManager {
    type Type = CoreConnection;
    type Error = ConnectError;

    async fn create(&self) -> Result<CoreConnection, ConnectError> {
        let (client, driver) = open_on_process_runtime(Arc::clone(&self.plan)).await?;
        let abort = driver.abort_handle();
        // Dropping the wrapper on a failed setup aborts the driver task.
        let client = ClientWrapper::new(client, driver);
        set_up_session(&client, self.protocol, &self.plan).await?;
        Ok(CoreConnection {
            client,
            driver: abort,
            state: ConnectionState::New,
        })
    }

    async fn recycle(
        &self,
        connection: &mut CoreConnection,
        metrics: &Metrics,
    ) -> RecycleResult<ConnectError> {
        if connection.client.is_closed() {
            return Err(RecycleError::message("the connection is closed"));
        }
        if let ConnectionAge::ReplaceAfter(limit) = self.max_age {
            if metrics.age() > limit {
                return Err(RecycleError::message(
                    "the connection reached its maximum age",
                ));
            }
        }
        match hand_out(self.idle_ping, connection.state, Instant::now()) {
            HandOut::Replace => Err(RecycleError::message(
                "the connection was discarded after its last use",
            )),
            HandOut::AsIs => Ok(()),
            HandOut::AfterPing => match connection.client.batch_execute(PING_SQL).await {
                Ok(()) => Ok(()),
                Err(error) => {
                    tracing::debug!(
                        target: LOG_TARGET,
                        %error,
                        "a pooled connection failed its ping: {error}; it is replaced",
                    );
                    Err(RecycleError::message(format!(
                        "the ping before checkout failed: {error}"
                    )))
                }
            },
        }
    }
}

/// Runs [`KEPT_SESSION_SETUP_SQL`] on a new connection of a pool that keeps
/// its session, within the plan's connection attempt bound. A pool in
/// PgBouncer transaction mode runs nothing.
///
/// Errors: [`ConnectFailure::SessionSetupFailed`] when the statement fails,
/// [`ConnectFailure::TimedOut`] when it does not end within the bound. The
/// caller drops the connection on either.
async fn set_up_session(
    client: &ClientWrapper,
    protocol: StatementProtocol,
    plan: &ConnectPlan,
) -> Result<(), ConnectError> {
    match protocol {
        StatementProtocol::UnnamedOnly => Ok(()),
        StatementProtocol::PreparedPerConnection => {
            let bound = plan.attempt_bound();
            let cause =
                match tokio::time::timeout(bound, client.batch_execute(KEPT_SESSION_SETUP_SQL))
                    .await
                {
                    Ok(Ok(())) => return Ok(()),
                    Ok(Err(error)) => ConnectFailure::SessionSetupFailed {
                        statement: KEPT_SESSION_SETUP_SQL,
                        error,
                    },
                    Err(_elapsed) => ConnectFailure::TimedOut { bound },
                };
            Err(ConnectError {
                masked_url: plan.masked_url().clone(),
                cause,
            })
        }
    }
}

/// Keeps `kept` connections open in `pool` (psycopg's pool maintenance of
/// `min_size`): opens the missing ones at once, then checks the open count
/// each [`KEPT_OPEN_CHECK_INTERVAL`]. Ends when the pool is closed or
/// dropped. A connection that fails to open is logged and tried again at the
/// next check.
async fn keep_open(
    pool: WeakPool<CoreManager>,
    kept: NonZeroU32,
    timeouts: Timeouts,
    role: PoolRole,
) {
    loop {
        let Some(strong) = pool.upgrade() else {
            return;
        };
        if strong.is_closed() {
            return;
        }
        match open_missing(&strong, kept, &timeouts).await {
            Ok(()) => {}
            Err(PoolError::Closed) => return,
            Err(error) => {
                tracing::debug!(
                    target: LOG_TARGET,
                    role = %role,
                    error = %error,
                    "a connection the pool keeps open did not open: {error}; it is tried again",
                );
            }
        }
        drop(strong);
        tokio::time::sleep(KEPT_OPEN_CHECK_INTERVAL).await;
    }
}

/// Opens connections until `pool` holds `kept` of them. A new connection
/// opens only when no pooled one is free, so the held free ones are taken
/// first; every connection taken here goes back to the pool at the end. At
/// most `kept` checkouts.
///
/// Errors: the first [`PoolError`] of a checkout; the connections taken so
/// far go back to the pool.
async fn open_missing(
    pool: &Pool<CoreManager>,
    kept: NonZeroU32,
    timeouts: &Timeouts,
) -> Result<(), PoolError<ConnectError>> {
    let kept = usize::try_from(kept.get()).unwrap_or(usize::MAX);
    let mut held: Vec<Object<CoreManager>> = Vec::with_capacity(kept);
    while pool.status().size < kept && held.len() < kept {
        held.push(pool.timeout_get(timeouts).await?);
    }
    drop(held);
    Ok(())
}

/// Drives one pooled connection until it closes.
/// Open a connection of `plan` and start its driver, both on the runtime of
/// the process ([`crate::process_runtime`]): the socket belongs to the
/// runtime that opened it, and the connection lives while its driver runs,
/// so the connection serves every runtime of the process and outlives the
/// runtime of the caller.
///
/// Errors: the [`ConnectError`] of the attempt;
/// [`ConnectFailure::RuntimeUnavailable`] when the process runtime cannot be
/// used (nothing was connected).
async fn open_on_process_runtime(
    plan: Arc<ConnectPlan>,
) -> Result<(tokio_postgres::Client, JoinHandle<()>), ConnectError> {
    let masked_url = plan.masked_url().clone();
    let opening = spawn_on_process_runtime(async move {
        let (client, connection) = open_connection(&plan).await?;
        let driver = tokio::spawn(drive_connection(connection));
        Ok::<_, ConnectError>((client, driver))
    })
    .map_err(|refusal| ConnectError {
        masked_url,
        cause: ConnectFailure::RuntimeUnavailable(refusal),
    })?;
    match opening.await {
        Ok(opened) => opened,
        // The process runtime lives in a static and never shuts down, so the
        // open is never cancelled; a panic in it is a defect, resumed here.
        Err(join) => match join.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(join) => unreachable!("the process runtime cancelled a connect: {join}"),
        },
    }
}

async fn drive_connection(connection: DriverConnection) {
    match connection.await {
        Ok(()) => {}
        Err(error) => {
            tracing::debug!(
                target: LOG_TARGET,
                %error,
                "a database connection ended with an error: {error}",
            );
        }
    }
}

/// The reserve gate of a pool.
enum ReserveGate {
    Ungated,
    KeepFree { batch_permits: Arc<Semaphore> },
}

/// A permit of the reserve gate, held for the life of a checkout.
enum GatePermit {
    NotGated,
    Held { _permit: OwnedSemaphorePermit },
}

/// The status of a pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolStatus {
    pub max_size: usize,
    pub open: usize,
    pub idle: usize,
    pub waiting: usize,
}

/// One core pool.
pub struct CorePool {
    pool: Pool<CoreManager>,
    plan: PoolPlan,
    connect: Arc<ConnectPlan>,
    gate: ReserveGate,
    /// Set once the statement-pooling check passed on this pool; a failure
    /// is not kept.
    pooling_check_passed: OnceCell<PoolingCheckPassed>,
}

impl CorePool {
    /// Builds a pool. Does no I/O: connections open at checkout. A plan that
    /// keeps connections open ([`KeptOpen::Connections`]) also starts the
    /// task that opens them (`keep_open`), on the current tokio runtime: it
    /// must be built inside one.
    pub fn new(plan: PoolPlan, connect: Arc<ConnectPlan>) -> CorePool {
        let manager = CoreManager {
            plan: Arc::clone(&connect),
            idle_ping: plan.idle_ping,
            max_age: plan.max_age,
            protocol: plan.protocol,
        };
        let config = PoolConfig {
            max_size: usize::try_from(plan.sizing.size()).unwrap_or(usize::MAX),
            timeouts: checkout_timeouts(plan.checkout_wait),
            queue_mode: QueueMode::Lifo,
        };
        let pool: Pool<CoreManager> = match Pool::builder(manager)
            .config(config)
            .runtime(Runtime::Tokio1)
            .build()
        {
            Ok(pool) => pool,
            Err(BuildError::NoRuntimeSpecified) => {
                unreachable!("the pool builder was given the tokio runtime")
            }
        };
        let gate = match plan.sizing.reserve() {
            Reserve::NoReserve => ReserveGate::Ungated,
            Reserve::KeepFree(_) => ReserveGate::KeepFree {
                batch_permits: Arc::new(Semaphore::new(
                    usize::try_from(plan.sizing.batch_limit()).unwrap_or(usize::MAX),
                )),
            },
        };
        match plan.sizing.kept_open() {
            KeptOpen::NoConnections => {}
            KeptOpen::Connections(kept) => {
                // On the process runtime, as the connections it keeps. When
                // that runtime cannot be used, no connection can open either:
                // each checkout names the refusal.
                match spawn_on_process_runtime(keep_open(
                    pool.weak(),
                    kept,
                    checkout_timeouts(plan.checkout_wait),
                    plan.role,
                )) {
                    Ok(_keeper) => {}
                    Err(refusal) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            role = ?plan.role,
                            %refusal,
                            "the pool keeps no connection open: the process runtime cannot be used",
                        );
                    }
                }
            }
        }
        CorePool {
            pool,
            plan,
            connect,
            gate,
            pooling_check_passed: OnceCell::new(),
        }
    }

    pub fn role(&self) -> PoolRole {
        self.plan.role
    }

    /// The size of the pool and its reserve.
    pub fn sizing(&self) -> PoolSizing {
        self.plan.sizing
    }

    /// The connection plan that each new connection of the pool opens with.
    #[cfg(test)]
    pub(crate) fn connect_plan(&self) -> &ConnectPlan {
        &self.connect
    }

    /// Checks out a connection.
    ///
    /// Waits at most `checkout_wait` (gate and pool together). Until the
    /// statement-pooling check has passed on this pool, runs it on the
    /// checked-out connection. Sends nothing else.
    ///
    /// Errors: [`CheckoutFailure`]. After an error nothing was sent and no
    /// connection is held.
    pub async fn checkout(
        &self,
        class: CheckoutClass,
    ) -> Result<PooledConnection, CheckoutFailure> {
        self.checkout_within(class, self.plan.checkout_wait).await
    }

    /// Checks out a connection as [`CorePool::checkout`] does, waiting at
    /// most `wait` or the pool's `checkout_wait`, whichever is shorter. A
    /// zero wait takes a free connection or fails at once.
    ///
    /// Errors: [`CheckoutFailure`]. After an error nothing was sent and no
    /// connection is held.
    pub async fn checkout_within(
        &self,
        class: CheckoutClass,
        wait: Duration,
    ) -> Result<PooledConnection, CheckoutFailure> {
        let wait = wait.min(self.plan.checkout_wait);
        let started = Instant::now();
        let permit = self.pass_gate(class, started, wait).await?;
        let remaining = wait.saturating_sub(started.elapsed());
        let object = self
            .pool
            .timeout_get(&checkout_timeouts(remaining))
            .await
            .map_err(|error| self.checkout_failure(error, started))?;
        let mut connection = PooledConnection {
            object,
            disposition: Disposition::Reusable,
            protocol: self.plan.protocol,
            connect: Arc::clone(&self.connect),
            _permit: permit,
        };
        let role = self.plan.role;
        let connect = &self.connect;
        let checked = &mut connection;
        let _passed = self
            .pooling_check_passed
            .get_or_try_init(move || run_pooling_check(checked, role, connect))
            .await?;
        Ok(connection)
    }

    /// Checks out a connection and runs one statement on it.
    pub async fn query(
        &self,
        class: CheckoutClass,
        statement: &BoundStatement<'_>,
        bound: StatementBound,
    ) -> Result<Vec<Row>, StatementFailure> {
        let mut connection = self
            .checkout(class)
            .await
            .map_err(|cause| not_sent(statement, cause))?;
        connection.query(statement, bound).await
    }

    /// Checks out a connection and runs one statement on it.
    pub async fn execute(
        &self,
        class: CheckoutClass,
        statement: &BoundStatement<'_>,
        bound: StatementBound,
    ) -> Result<u64, StatementFailure> {
        let mut connection = self
            .checkout(class)
            .await
            .map_err(|cause| not_sent(statement, cause))?;
        connection.execute(statement, bound).await
    }

    /// Closes the pool: every later checkout fails with `PoolClosed`. Checked
    /// out connections end when their guards drop.
    pub fn close(&self) {
        self.pool.close();
        match &self.gate {
            ReserveGate::Ungated => {}
            ReserveGate::KeepFree { batch_permits } => batch_permits.close(),
        }
    }

    pub fn status(&self) -> PoolStatus {
        let status = self.pool.status();
        PoolStatus {
            max_size: status.max_size,
            open: status.size,
            idle: status.available,
            waiting: status.waiting,
        }
    }

    async fn pass_gate(
        &self,
        class: CheckoutClass,
        started: Instant,
        wait: Duration,
    ) -> Result<GatePermit, CheckoutFailure> {
        let batch_permits = match (&self.gate, class) {
            (ReserveGate::Ungated, _) | (ReserveGate::KeepFree { .. }, CheckoutClass::Reserved) => {
                return Ok(GatePermit::NotGated)
            }
            (ReserveGate::KeepFree { batch_permits }, CheckoutClass::Batch) => {
                Arc::clone(batch_permits)
            }
        };
        match tokio::time::timeout(wait, batch_permits.acquire_owned()).await {
            Ok(Ok(permit)) => Ok(GatePermit::Held { _permit: permit }),
            Ok(Err(_closed)) => Err(CheckoutFailure::PoolClosed {
                role: self.plan.role,
            }),
            Err(_elapsed) => Err(CheckoutFailure::TimedOut {
                role: self.plan.role,
                waited: started.elapsed(),
            }),
        }
    }

    fn checkout_failure(
        &self,
        error: PoolError<ConnectError>,
        started: Instant,
    ) -> CheckoutFailure {
        let role = self.plan.role;
        match error {
            PoolError::Timeout(TimeoutType::Wait | TimeoutType::Create | TimeoutType::Recycle) => {
                CheckoutFailure::TimedOut {
                    role,
                    waited: started.elapsed(),
                }
            }
            PoolError::Backend(cause) => CheckoutFailure::ConnectFailed { role, cause },
            PoolError::Closed => CheckoutFailure::PoolClosed { role },
            PoolError::NoRuntimeSpecified => {
                unreachable!("the {role} pool was built with the tokio runtime")
            }
            PoolError::PostCreateHook(_) => {
                unreachable!("the {role} pool has no post-create hook")
            }
        }
    }
}

/// deadpool's timeouts for one checkout. Connect attempts carry their own
/// bound, so no create timeout is set here.
fn checkout_timeouts(wait: Duration) -> Timeouts {
    Timeouts {
        wait: Some(wait),
        create: None,
        recycle: Some(IDLE_PING_BOUND),
    }
}

fn not_sent(statement: &BoundStatement<'_>, cause: CheckoutFailure) -> StatementFailure {
    StatementFailure::NotSent {
        statement: statement.statement().name(),
        cause: NotSentCause::Checkout(cause),
    }
}

/// What happens to a connection when its guard drops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Disposition {
    /// Back to the pool, with the check-in time recorded.
    Reusable,
    /// Out of the pool: the state is unknown (a lost reply, a statement that
    /// may still run, a broken session).
    Discard,
}

/// A checked-out connection. Dropping it returns the connection to its pool,
/// or discards it after a failure that left its state unknown.
pub struct PooledConnection {
    object: Object<CoreManager>,
    disposition: Disposition,
    protocol: StatementProtocol,
    connect: Arc<ConnectPlan>,
    /// Released after the connection went back to the pool (fields drop in
    /// order).
    _permit: GatePermit,
}

impl PooledConnection {
    /// Runs one statement and reads every row.
    pub async fn query(
        &mut self,
        statement: &BoundStatement<'_>,
        bound: StatementBound,
    ) -> Result<Vec<Row>, StatementFailure> {
        let name = statement.statement().name();
        self.ensure_usable(name)?;
        let bounding = self.bounding(bound);
        let outcome = run_bounded(
            query_rows(&self.object.client, self.protocol, statement),
            bounding,
        )
        .await;
        self.settle(name, outcome)
    }

    /// Runs one statement and reads the count of affected rows.
    pub async fn execute(
        &mut self,
        statement: &BoundStatement<'_>,
        bound: StatementBound,
    ) -> Result<u64, StatementFailure> {
        let name = statement.statement().name();
        self.ensure_usable(name)?;
        let bounding = self.bounding(bound);
        let outcome = run_bounded(
            execute_count(&self.object.client, self.protocol, statement),
            bounding,
        )
        .await;
        self.settle(name, outcome)
    }

    /// The driver client, for the statement-pooling check and the listener's
    /// probes. Statements go through [`PooledConnection::query`] and
    /// [`PooledConnection::execute`].
    pub(crate) fn client(&self) -> &tokio_postgres::Client {
        &self.object.client
    }

    /// Marks the connection to be discarded when the guard drops.
    pub(crate) fn discard(&mut self) {
        self.disposition = Disposition::Discard;
    }

    /// Refuses to send on a discarded or closed connection.
    fn ensure_usable(&mut self, statement: &'static str) -> Result<(), StatementFailure> {
        match (self.disposition, self.object.client.is_closed()) {
            (Disposition::Discard, _) => Err(StatementFailure::NotSent {
                statement,
                cause: NotSentCause::ConnectionDiscarded,
            }),
            (Disposition::Reusable, true) => {
                self.discard();
                Err(StatementFailure::NotSent {
                    statement,
                    cause: NotSentCause::ConnectionClosed,
                })
            }
            (Disposition::Reusable, false) => Ok(()),
        }
    }

    /// Runs one transaction-control command (simple query protocol, no
    /// parameters), with the same three outcomes as a statement.
    pub(crate) async fn command(
        &mut self,
        command: TransactionCommand,
    ) -> Result<(), StatementFailure> {
        let name = command.name();
        self.ensure_usable(name)?;
        let outcome = run_bounded(
            run_command(&self.object.client, command.sql()),
            Bounding::ConnectionTimeouts,
        )
        .await;
        self.settle(name, outcome)
    }

    /// Runs a script (simple query protocol, no parameters) with the same
    /// three outcomes as a statement. [`crate::db::Transaction::run_script`]
    /// is the public form: a script runs inside a transaction.
    pub(crate) async fn run_script(
        &mut self,
        script: &SqlScript,
        bound: StatementBound,
    ) -> Result<(), StatementFailure> {
        let name = script.name();
        self.ensure_usable(name)?;
        let bounding = self.bounding(bound);
        let outcome = run_bounded(run_command(&self.object.client, script.sql()), bounding).await;
        self.settle(name, outcome)
    }

    /// Runs one partition DDL statement (simple query protocol, no
    /// parameters) with the same three outcomes as a statement. A refusal
    /// keeps the server error, so a held lock (`55P03`, `lock_not_available`)
    /// and a statement timeout (`57014`) stay apart.
    ///
    /// The statement runs in autocommit: an open [`crate::db::Transaction`]
    /// borrows this connection, so no transaction is open while this method
    /// runs. Session settings (`statement_timeout`, `lock_timeout`) apply as
    /// the caller set them.
    pub async fn run_partition_ddl(
        &mut self,
        ddl: &PartitionDdl,
        bound: StatementBound,
    ) -> Result<(), StatementFailure> {
        let name = ddl.name();
        self.ensure_usable(name)?;
        let bounding = self.bounding(bound);
        let sql = ddl.sql();
        let outcome = run_bounded(run_command(&self.object.client, &sql), bounding).await;
        self.settle(name, outcome)
    }

    fn bounding(&self, bound: StatementBound) -> Bounding {
        match bound {
            StatementBound::ConnectionTimeouts => Bounding::ConnectionTimeouts,
            StatementBound::CancelAfter(limit) => Bounding::CancelAfter {
                limit,
                target: CancelTarget {
                    token: self.object.client.cancel_token(),
                    tls: self.connect.tls().clone(),
                    bound: self.connect.attempt_bound().min(CANCEL_REQUEST_BOUND),
                },
            },
        }
    }

    fn settle<T>(
        &mut self,
        statement: &'static str,
        outcome: BoundedOutcome<T>,
    ) -> Result<T, StatementFailure> {
        match outcome {
            BoundedOutcome::Completed(Ok(value))
            | BoundedOutcome::CompletedAfterCancel {
                result: Ok(value), ..
            } => Ok(value),
            BoundedOutcome::Completed(Err(attempt)) => {
                Err(self.classify(statement, attempt, CancelContext::NotCancelled))
            }
            BoundedOutcome::CompletedAfterCancel {
                result: Err(attempt),
                bound,
            } => Err(self.classify(statement, attempt, CancelContext::CancelledAfter(bound))),
            BoundedOutcome::Abandoned { bound, cancel } => {
                self.discard();
                Err(StatementFailure::OutcomeUnknown {
                    statement,
                    cause: UnknownOutcomeCause::NoReplyWithinBound {
                        bound,
                        grace: CANCEL_GRACE,
                        cancel,
                    },
                })
            }
        }
    }

    /// Sorts a driver error into the three outcomes.
    ///
    /// - A server ERROR: refused; the connection stays usable.
    /// - A server FATAL or PANIC: the session ended. Before the run it is
    ///   `NotSent`; during the run the outcome is unknown.
    /// - Any other driver error: the connection is discarded. Before the run
    ///   (prepare) it is `NotSent`; during the run the outcome is unknown.
    fn classify(
        &mut self,
        statement: &'static str,
        attempt: AttemptError,
        cancel: CancelContext,
    ) -> StatementFailure {
        let server_error = attempt.error.as_db_error().cloned();
        let scope = server_error.as_ref().map(error_scope);
        match (attempt.stage, server_error, scope) {
            (Stage::Prepare | Stage::Run, Some(error), Some(ErrorScope::Statement)) => {
                let cause = match (cancel, error.code() == &SqlState::QUERY_CANCELED) {
                    (CancelContext::CancelledAfter(bound), true) => {
                        RefusalCause::CancelledAfterBound { bound, error }
                    }
                    (CancelContext::CancelledAfter(_) | CancelContext::NotCancelled, _) => {
                        RefusalCause::Server(error)
                    }
                };
                StatementFailure::StatementRefused { statement, cause }
            }
            (Stage::Prepare, _, _) => {
                self.discard();
                StatementFailure::NotSent {
                    statement,
                    cause: NotSentCause::PrepareFailed(attempt.error),
                }
            }
            (Stage::Run, Some(error), Some(ErrorScope::Session)) => {
                self.discard();
                StatementFailure::OutcomeUnknown {
                    statement,
                    cause: UnknownOutcomeCause::SessionTerminated(error),
                }
            }
            (Stage::Run, _, _) => {
                self.discard();
                let cause = match self.object.client.is_closed() {
                    true => UnknownOutcomeCause::ConnectionLost(attempt.error),
                    false => UnknownOutcomeCause::DriverFailed(attempt.error),
                };
                StatementFailure::OutcomeUnknown { statement, cause }
            }
        }
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        match self.disposition {
            Disposition::Reusable => {
                self.object.state = ConnectionState::ReturnedAt(Instant::now());
            }
            Disposition::Discard => {
                self.object.driver.abort();
                self.object.state = ConnectionState::Discarded;
            }
        }
    }
}

/// Whether the core cancelled the statement after its bound.
#[derive(Clone, Copy, Debug)]
enum CancelContext {
    NotCancelled,
    CancelledAfter(Duration),
}

/// Where a cancel request goes.
struct CancelTarget {
    token: CancelToken,
    tls: MakeRustlsConnect,
    bound: Duration,
}

impl CancelTarget {
    /// Sends the cancel request on a new connection, within `bound`.
    async fn send(self) -> CancelRequestOutcome {
        match tokio::time::timeout(self.bound, self.token.cancel_query(self.tls)).await {
            Ok(Ok(())) => CancelRequestOutcome::Sent,
            Ok(Err(error)) => CancelRequestOutcome::Failed(error),
            Err(_elapsed) => CancelRequestOutcome::TimedOut { bound: self.bound },
        }
    }
}

/// The bound of one run, with what a cancel needs.
enum Bounding {
    ConnectionTimeouts,
    CancelAfter {
        limit: Duration,
        target: CancelTarget,
    },
}

/// How a bounded run ended.
enum BoundedOutcome<T> {
    /// The reply came within the bound.
    Completed(Result<T, AttemptError>),
    /// The reply came within the cancel grace after a cancel request.
    CompletedAfterCancel {
        result: Result<T, AttemptError>,
        bound: Duration,
    },
    /// No reply within the bound and the grace.
    Abandoned {
        bound: Duration,
        cancel: CancelRequestOutcome,
    },
}

async fn run_bounded<T, F>(future: F, bounding: Bounding) -> BoundedOutcome<T>
where
    F: std::future::Future<Output = Result<T, AttemptError>>,
{
    match bounding {
        Bounding::ConnectionTimeouts => BoundedOutcome::Completed(future.await),
        Bounding::CancelAfter { limit, target } => {
            tokio::pin!(future);
            match tokio::time::timeout(limit, &mut future).await {
                Ok(result) => BoundedOutcome::Completed(result),
                Err(_elapsed) => {
                    let cancel = target.send().await;
                    match tokio::time::timeout(CANCEL_GRACE, &mut future).await {
                        Ok(result) => BoundedOutcome::CompletedAfterCancel {
                            result,
                            bound: limit,
                        },
                        Err(_elapsed) => BoundedOutcome::Abandoned {
                            bound: limit,
                            cancel,
                        },
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keep(count: u32) -> Reserve {
        Reserve::KeepFree(NonZeroU32::new(count).expect("non-zero reserve"))
    }

    #[test]
    fn a_pool_without_reserve_gives_every_connection_to_batches() {
        let sizing = PoolSizing::new(10, Reserve::NoReserve).expect("valid sizing");
        assert_eq!(sizing.size(), 10);
        assert_eq!(sizing.batch_limit(), 10);
    }

    #[test]
    fn the_worker_reserve_keeps_two_connections_free() {
        let sizing =
            PoolSizing::new(10, Reserve::KeepFree(WORKER_RESERVED_CONNECTIONS)).expect("valid");
        assert_eq!(sizing.batch_limit(), 8);
    }

    #[test]
    fn the_reserve_must_leave_one_connection_for_batches() {
        assert_eq!(
            PoolSizing::new(2, keep(2)),
            Err(PoolSizingError::ReserveNotBelowSize {
                size: 2,
                reserve: 2
            }),
        );
        assert_eq!(
            PoolSizing::new(2, keep(3)),
            Err(PoolSizingError::ReserveNotBelowSize {
                size: 2,
                reserve: 3
            }),
        );
        assert_eq!(
            PoolSizing::new(3, keep(2)).map(|sizing| sizing.batch_limit()),
            Ok(1)
        );
    }

    #[test]
    fn size_bounds_are_checked() {
        assert_eq!(
            PoolSizing::new(0, Reserve::NoReserve),
            Err(PoolSizingError::SizeZero)
        );
        assert_eq!(
            PoolSizing::new(POSTGRES_MAX_BACKENDS + 1, Reserve::NoReserve),
            Err(PoolSizingError::SizeAboveLimit {
                size: POSTGRES_MAX_BACKENDS + 1,
                limit: POSTGRES_MAX_BACKENDS
            }),
        );
        assert!(PoolSizing::new(POSTGRES_MAX_BACKENDS, Reserve::NoReserve).is_ok());
    }

    #[test]
    fn a_pool_keeps_open_at_most_its_size() {
        let sizing = PoolSizing::new(3, Reserve::NoReserve).expect("valid sizing");
        assert_eq!(sizing.kept_open(), KeptOpen::NoConnections);
        let three = KeptOpen::Connections(NonZeroU32::new(3).expect("non-zero"));
        assert_eq!(
            sizing.keeping_open(three).map(|kept| kept.kept_open()),
            Ok(three)
        );
        assert_eq!(
            sizing.keeping_open(KeptOpen::Connections(NonZeroU32::new(4).expect("non-zero"))),
            Err(PoolSizingError::KeptOpenAboveSize { size: 3, kept: 4 }),
        );
        assert_eq!(sizing.keeping_open(KeptOpen::NoConnections), Ok(sizing));
    }

    #[test]
    fn every_checkout_pings_each_pooled_connection() {
        let returned_at = Instant::now();
        let at_once = returned_at;
        assert_eq!(
            hand_out(IdlePing::EveryCheckout, ConnectionState::New, at_once),
            HandOut::AfterPing
        );
        assert_eq!(
            hand_out(
                IdlePing::EveryCheckout,
                ConnectionState::ReturnedAt(returned_at),
                at_once
            ),
            HandOut::AfterPing
        );
        assert_eq!(
            hand_out(IdlePing::EveryCheckout, ConnectionState::Discarded, at_once),
            HandOut::Replace
        );
    }

    #[test]
    fn the_idle_ping_pings_only_after_the_idle_limit() {
        let returned_at = Instant::now();
        let limit = DEFAULT_IDLE_PING_AFTER;
        let ping = IdlePing::AfterIdle(limit);
        assert_eq!(
            hand_out(
                ping,
                ConnectionState::ReturnedAt(returned_at),
                returned_at + limit
            ),
            HandOut::AsIs
        );
        assert_eq!(
            hand_out(
                ping,
                ConnectionState::ReturnedAt(returned_at),
                returned_at + limit + Duration::from_millis(1)
            ),
            HandOut::AfterPing
        );
        assert_eq!(
            hand_out(ping, ConnectionState::New, returned_at),
            HandOut::AsIs
        );
        assert_eq!(
            hand_out(ping, ConnectionState::Discarded, returned_at),
            HandOut::Replace
        );
    }

    #[test]
    fn no_ping_hands_out_every_kept_connection() {
        let returned_at = Instant::now();
        let later = returned_at + Duration::from_secs(3600);
        assert_eq!(
            hand_out(
                IdlePing::Off,
                ConnectionState::ReturnedAt(returned_at),
                later
            ),
            HandOut::AsIs
        );
        assert_eq!(
            hand_out(IdlePing::Off, ConnectionState::New, later),
            HandOut::AsIs
        );
        assert_eq!(
            hand_out(IdlePing::Off, ConnectionState::Discarded, later),
            HandOut::Replace
        );
    }

    #[tokio::test]
    async fn a_bounded_run_that_ends_in_time_is_completed() {
        let outcome = run_bounded(
            async { Ok::<u64, AttemptError>(7) },
            Bounding::ConnectionTimeouts,
        )
        .await;
        assert!(matches!(outcome, BoundedOutcome::Completed(Ok(7))));
    }
}

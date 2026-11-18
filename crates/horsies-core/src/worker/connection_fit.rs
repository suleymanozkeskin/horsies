//! Whether the connections of one worker of a profile fit the server
//! (Python `python/horsies/core/worker/profile_fit.py`, the connection part).
//!
//! One worker of a profile opens connections itself: its pool (the resolved
//! `WorkerProfile.pool`), its session pool when the session URL differs, its
//! listener, its runner heartbeat connection, its liveness lock connection,
//! and the pool of each child.
//! The sum is compared with the slots that an
//! ordinary role can use on the server: `max_connections` minus
//! `superuser_reserved_connections` minus `reserved_connections`
//! (PostgreSQL 16 and later). `horsies check --live` reports a profile whose
//! sum is above the usable slots as `CHECK_WORKER_CONNECTIONS`. Through a
//! PgBouncer transaction pool nothing is read: pooled client connections do
//! not map to server connections.
//!
//! The terms are the connections that the core opens for a worker process:
//! - worker pool: [`PostgresConfig::worker_process_config`] sizes the worker
//!   broker to the resolved parent pool with no overflow, and the broker's
//!   pool holds `pool_size + max_overflow`. The reserve of the reaper and the
//!   per-task writes ([`crate::db::WORKER_RESERVED_CONNECTIONS`]) is kept
//!   inside that size, so it adds no term;
//! - session pool: a pool of its own on `session_database_url` only when that
//!   URL differs from `database_url`, sized as the worker pool (the broker
//!   builds both from one config; Python's session engine is sized the same
//!   way and Python does not count it);
//! - listener: one dedicated connection on the session URL carries every
//!   LISTEN of the process ([`crate::db::Listener`]); Python's listener holds
//!   two;
//! - runner heartbeat: a pool of one connection of its own
//!   ([`crate::worker::runner_beats::runner_beat_pool`]);
//! - liveness lock: one dedicated connection on the session URL that holds
//!   the worker's session advisory lock ([`crate::worker::liveness`]).
//!   Python has no such connection; with the core listener's one connection
//!   in place of Python's two, a worker opens as many connections as Python's.
//!
//! Task code that sends opens the `task_send_pool` of its child kind in its
//! child, for each event loop that it sends from. Whether it sends is known
//! only at run time, so it is not compared; each error states it.
//!
//! [`read_server_connection_limit`] does the one read. The comparison and
//! the errors are pure.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::broker::core_rows::{column, one_row, StatementLog};
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::config::worker_profile::{
    profile_path, quoted_profile_name, IsolatedSetting, SharedAsyncSetting, SharedThreadSetting,
    WorkerProfile,
};
use crate::core::config::{AppConfig, PostgresConfig};
use crate::core::error::{ErrorCode, HorsiesError};
use crate::db::{CheckoutClass, CorePool, StatementBound, TypedStatement};
use crate::handle::CoreHandle;

/// Connections of the process listener of a worker: one connection carries
/// every LISTEN of the process.
pub const LISTENER_CONNECTIONS_PER_WORKER: u32 = 1;

/// The connection over which the worker process writes the runner
/// heartbeats of the RUNNING tasks of its children.
pub const RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER: u32 = 1;

/// The connection on the session URL that holds the worker's liveness lock
/// for the life of the process.
pub const LIVENESS_LOCK_CONNECTIONS_PER_WORKER: u32 = 1;

/// The longest wait for the server to answer the connection limit read
/// (Python `CONNECTION_LIMIT_READ_TIMEOUT_S`).
pub const CONNECTION_LIMIT_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Python's statement (`python/horsies/core/worker/profile_fit.py:85-92`).
/// `reserved_connections` exists from PostgreSQL 16; before it, no slot is
/// reserved for it.
const CONNECTION_LIMIT_SQL: &str = "\
SELECT current_setting('max_connections')::int AS max_connections,
       current_setting('superuser_reserved_connections')::int AS superuser_reserved,
       COALESCE(current_setting('reserved_connections', true), '0')::int AS reserved";

/// [`CONNECTION_LIMIT_SQL`]; it takes no parameter.
static CONNECTION_LIMIT: TypedStatement =
    TypedStatement::new("server_connection_limit", CONNECTION_LIMIT_SQL, &[]);

/// What to change when a profile needs more connections than the server
/// has (Python `CONNECTIONS_HELP`).
pub const CONNECTIONS_HELP: &str = "lower the child counts, the pool max_size of a mode \
(isolated.pool, shared_async.pool, shared_thread.pool) or the parent pool max_size \
(pool=ParentPool(max_size=PoolSize(connections=N))), raise max_connections, or send the \
pooled connections through a transaction pooler (pgbouncer_transaction_mode=True)";

/// The help of an unreadable limit (Python `_check_worker_connections`).
const LIMIT_UNREADABLE_HELP: &str = "the worker profiles were not compared with \
max_connections; check that the role can run current_setting('max_connections')";

/// The connection limit of the server. `reserved` is
/// `superuser_reserved_connections` plus `reserved_connections`: slots that
/// an ordinary role cannot use. Only [`server_connection_limit`] builds one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerConnectionLimit {
    max_connections: u32,
    reserved: u32,
}

impl ServerConnectionLimit {
    pub fn max_connections(&self) -> u32 {
        self.max_connections
    }

    pub fn reserved(&self) -> u32 {
        self.reserved
    }

    /// The slots that an ordinary role can use; 0 when the reserved slots
    /// take every slot.
    pub fn usable(&self) -> u32 {
        self.max_connections.saturating_sub(self.reserved)
    }
}

/// Why the connection limit was not read. The profiles are then not
/// compared; nothing is guessed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionLimitUnreadable {
    /// The pools could not open, no connection could be checked out, the
    /// statement failed, or its row did not read.
    #[error("reading the server connection limit failed: {0}")]
    Database(BrokerError),
    /// The server did not answer within the bound.
    #[error("the server did not answer the connection limit read within {bound:?}")]
    TimedOut { bound: Duration },
    /// A setting read as a negative number.
    #[error("the server setting {setting} reads as {value}, below 0")]
    NegativeSetting { setting: &'static str, value: i32 },
}

/// The limit from the three settings the statement reads. Pure.
///
/// Errors: [`ConnectionLimitUnreadable::NegativeSetting`] naming the first
/// negative setting.
pub fn server_connection_limit(
    max_connections: i32,
    superuser_reserved: i32,
    reserved: i32,
) -> Result<ServerConnectionLimit, ConnectionLimitUnreadable> {
    let non_negative = |setting: &'static str, value: i32| {
        u32::try_from(value)
            .map_err(|_negative| ConnectionLimitUnreadable::NegativeSetting { setting, value })
    };
    let max_connections = non_negative("max_connections", max_connections)?;
    let superuser_reserved = non_negative("superuser_reserved_connections", superuser_reserved)?;
    let reserved = non_negative("reserved_connections", reserved)?;
    Ok(ServerConnectionLimit {
        max_connections,
        reserved: superuser_reserved.saturating_add(reserved),
    })
}

/// Reads the connection limit with one statement in autocommit on the main
/// core pool of the broker ([`read_connection_limit_on`]). Writes nothing.
///
/// Errors: [`ConnectionLimitUnreadable`]. A caller reports it; it does not
/// retry here.
pub async fn read_server_connection_limit(
    broker: &PostgresBroker,
) -> Result<ServerConnectionLimit, ConnectionLimitUnreadable> {
    let (open, _read) = broker
        .core_pools_for_write()
        .map_err(ConnectionLimitUnreadable::Database)?;
    read_connection_limit_on(open.main(), broker.statement_log()).await
}

/// Reads the connection limit with one statement on one connection of
/// `pool` that passes the reserve gate ([`CheckoutClass::Batch`]), in
/// autocommit, waiting at most [`CONNECTION_LIMIT_READ_TIMEOUT`] for the
/// checkout and the answer together. Writes nothing.
///
/// Errors: [`ConnectionLimitUnreadable`].
pub(crate) async fn read_connection_limit_on(
    pool: &CorePool,
    log: StatementLog,
) -> Result<ServerConnectionLimit, ConnectionLimitUnreadable> {
    let statement = &CONNECTION_LIMIT;
    let bound = statement.bind().finish();
    log.log(statement);
    let read = pool.query(
        CheckoutClass::Batch,
        &bound,
        StatementBound::ConnectionTimeouts,
    );
    let rows = match tokio::time::timeout(CONNECTION_LIMIT_READ_TIMEOUT, read).await {
        Err(_elapsed) => {
            return Err(ConnectionLimitUnreadable::TimedOut {
                bound: CONNECTION_LIMIT_READ_TIMEOUT,
            })
        }
        Ok(Err(failure)) => {
            return Err(ConnectionLimitUnreadable::Database(BrokerError::Statement(
                failure,
            )))
        }
        Ok(Ok(rows)) => rows,
    };
    let settings = one_row(rows, statement).and_then(|row| {
        Ok((
            column::<i32>(&row, statement, "max_connections")?,
            column::<i32>(&row, statement, "superuser_reserved")?,
            column::<i32>(&row, statement, "reserved")?,
        ))
    });
    match settings {
        Ok((max_connections, superuser_reserved, reserved)) => {
            server_connection_limit(max_connections, superuser_reserved, reserved)
        }
        Err(decode) => Err(ConnectionLimitUnreadable::Database(BrokerError::RowDecode(
            decode,
        ))),
    }
}

/// An execution mode with children.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildMode {
    Isolated,
    SharedAsync,
    SharedThread,
}

impl ChildMode {
    /// The field of the mode in `WorkerProfile`.
    pub const fn field(self) -> &'static str {
        match self {
            Self::Isolated => "isolated",
            Self::SharedAsync => "shared_async",
            Self::SharedThread => "shared_thread",
        }
    }

    /// The mode in words, as the message writes it.
    const fn words(self) -> &'static str {
        match self {
            Self::Isolated => "isolated",
            Self::SharedAsync => "shared async",
            Self::SharedThread => "shared thread",
        }
    }
}

/// The pools of the `children` children of one mode: each opens up to
/// `per_child` connections, the resolved pool `max_size` of the mode. Task
/// code that sends opens up to `task_send_pool` more in its child, for each
/// event loop that it sends from. `children` is 1 or more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModePools {
    pub mode: ChildMode,
    pub children: u32,
    pub per_child: u32,
    pub task_send_pool: u32,
}

impl ModePools {
    pub fn connections(&self) -> u64 {
        u64::from(self.children) * u64::from(self.per_child)
    }

    /// The connections that task code adds with one sending loop per child.
    pub fn task_sends(&self) -> u64 {
        u64::from(self.children) * u64::from(self.task_send_pool)
    }

    fn label(&self) -> String {
        format!("{} {} children", self.children, self.mode.words())
    }

    /// The term of the mode in the connection formula.
    fn term(&self) -> String {
        format!(
            "{} x {}.pool max_size {}",
            self.label(),
            self.mode.field(),
            self.per_child
        )
    }

    /// The term of the mode in the task-send ceiling.
    fn task_send_term(&self) -> String {
        format!(
            "{} x {}.task_send_pool {}",
            self.label(),
            self.mode.field(),
            self.task_send_pool
        )
    }
}

/// The session pool of a worker process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPoolTerm {
    /// `session_database_url` is absent or equal to `database_url`: session
    /// work takes connections of the worker pool, so there is no term.
    SharedWithWorkerPool,
    /// A pool of its own on `session_database_url` of up to `max_size`
    /// connections.
    Separate { max_size: u32 },
}

impl SessionPoolTerm {
    pub fn connections(&self) -> u64 {
        match self {
            Self::SharedWithWorkerPool => 0,
            Self::Separate { max_size } => u64::from(*max_size),
        }
    }

    fn term(&self) -> Option<String> {
        match self {
            Self::SharedWithWorkerPool => None,
            Self::Separate { max_size } => Some(format!("session pool max_size {max_size}")),
        }
    }
}

/// The Postgres connections of one worker, by part. `total` counts the
/// connections that the worker opens itself; `with_task_sends` adds one
/// sending loop of task code in each child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConnectionNeed {
    pub parent_pool: u32,
    pub session_pool: SessionPoolTerm,
    pub listener: u32,
    pub runner_heartbeat: u32,
    pub liveness_lock: u32,
    /// One entry for each mode with children: isolated, shared async, shared
    /// thread.
    pub child_pools: Vec<ModePools>,
}

impl WorkerConnectionNeed {
    pub fn total(&self) -> u64 {
        u64::from(self.parent_pool)
            + self.session_pool.connections()
            + u64::from(self.listener)
            + u64::from(self.runner_heartbeat)
            + u64::from(self.liveness_lock)
            + self
                .child_pools
                .iter()
                .map(ModePools::connections)
                .sum::<u64>()
    }

    pub fn task_sends(&self) -> u64 {
        self.child_pools.iter().map(ModePools::task_sends).sum()
    }

    pub fn with_task_sends(&self) -> u64 {
        self.total() + self.task_sends()
    }

    /// The sum, term by term (Python `WorkerConnectionNeed.formula`).
    pub fn formula(&self) -> String {
        let terms: Vec<String> = [
            Some(format!("parent pool max_size {}", self.parent_pool)),
            self.session_pool.term(),
            Some(format!("listener {}", self.listener)),
            Some(format!("runner heartbeat {}", self.runner_heartbeat)),
            Some(format!("liveness lock {}", self.liveness_lock)),
        ]
        .into_iter()
        .flatten()
        .chain(self.child_pools.iter().map(ModePools::term))
        .collect();
        format!("{} = {}", terms.join(" + "), self.total())
    }

    /// What `total` does not count, term by term (Python
    /// `WorkerConnectionNeed.task_sends_note`).
    pub fn task_sends_note(&self) -> String {
        let terms: Vec<String> = self
            .child_pools
            .iter()
            .map(ModePools::task_send_term)
            .collect();
        format!(
            "not counted: task code that sends opens a pool of the task_send_pool of its \
             child kind in its child, for each event loop that it sends from; with one such \
             loop in each child, {} = {} more, {} in all",
            terms.join(" + "),
            self.task_sends(),
            self.with_task_sends()
        )
    }
}

fn isolated_pools(isolated: &IsolatedSetting) -> Option<ModePools> {
    match isolated {
        IsolatedSetting::IsolatedChildren(children) => Some(ModePools {
            mode: ChildMode::Isolated,
            children: children.count.get(),
            per_child: children.pool.max_size.connections.get(),
            task_send_pool: children.task_send_pool.connections.get(),
        }),
        IsolatedSetting::NoIsolatedChildren {} => None,
    }
}

fn shared_async_pools(shared_async: &SharedAsyncSetting) -> Option<ModePools> {
    match shared_async {
        SharedAsyncSetting::SharedAsyncChildren(children) => Some(ModePools {
            mode: ChildMode::SharedAsync,
            children: children.count.get(),
            per_child: children.resolved_max_pool_size().get(),
            task_send_pool: children.task_send_pool.connections.get(),
        }),
        SharedAsyncSetting::NoSharedAsyncChildren {} => None,
    }
}

fn shared_thread_pools(shared_thread: &SharedThreadSetting) -> Option<ModePools> {
    match shared_thread {
        SharedThreadSetting::SharedThreadChildren(children) => Some(ModePools {
            mode: ChildMode::SharedThread,
            children: children.count.get(),
            per_child: children.resolved_max_pool_size().get(),
            task_send_pool: children.task_send_pool.connections.get(),
        }),
        SharedThreadSetting::NoSharedThreadChildren {} => None,
    }
}

/// The session pool that a worker process of `broker` opens beside a worker
/// pool of `parent_pool` connections. Pure.
fn session_pool_term(broker: &PostgresConfig, parent_pool: u32) -> SessionPoolTerm {
    match broker.effective_session_database_url() == broker.database_url {
        true => SessionPoolTerm::SharedWithWorkerPool,
        false => SessionPoolTerm::Separate {
            max_size: parent_pool,
        },
    }
}

/// The connections of one worker of this profile on the database of
/// `broker` (Python `worker_connection_need`, with the session pool and the
/// liveness lock). Pure.
pub fn worker_connection_need(
    profile: &WorkerProfile,
    broker: &PostgresConfig,
) -> WorkerConnectionNeed {
    let parent_pool = profile.resolved_parent_pool().get();
    WorkerConnectionNeed {
        parent_pool,
        session_pool: session_pool_term(broker, parent_pool),
        listener: LISTENER_CONNECTIONS_PER_WORKER,
        runner_heartbeat: RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER,
        liveness_lock: LIVENESS_LOCK_CONNECTIONS_PER_WORKER,
        child_pools: [
            isolated_pools(&profile.isolated),
            shared_async_pools(&profile.shared_async),
            shared_thread_pools(&profile.shared_thread),
        ]
        .into_iter()
        .flatten()
        .collect(),
    }
}

/// How the connections of one worker of a profile compare with the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionFit {
    /// The worker's own connections fit in the usable slots. Task code that
    /// sends is not compared.
    Fits {
        need: WorkerConnectionNeed,
        limit: ServerConnectionLimit,
    },
    /// The worker's own connections are more than the usable slots.
    ExceedsServer {
        need: WorkerConnectionNeed,
        limit: ServerConnectionLimit,
    },
}

impl ConnectionFit {
    /// The fact in words (Python `ConnectionsFit.describe`,
    /// `ConnectionsExceedServer.describe`). A worker's boot log states it;
    /// the error of an excess starts with it.
    pub fn describe(&self) -> String {
        match self {
            Self::Fits { need, limit } => fits_text(need, *limit),
            Self::ExceedsServer { need, limit } => exceeds_server_text(need, *limit),
        }
    }
}

fn fits_text(need: &WorkerConnectionNeed, limit: ServerConnectionLimit) -> String {
    format!(
        "one worker opens up to {} Postgres connections itself, of {} usable on the server; {}",
        need.total(),
        limit.usable(),
        need.task_sends_note()
    )
}

fn exceeds_server_text(need: &WorkerConnectionNeed, limit: ServerConnectionLimit) -> String {
    format!(
        "one worker opens up to {} Postgres connections itself, not counting task code; the \
         server has {} usable slots (max_connections {} - reserved {})",
        need.total(),
        limit.usable(),
        limit.max_connections(),
        limit.reserved()
    )
}

/// Compares the connections that one worker of the profile opens itself
/// with the usable slots (Python `compare_connections`). Pure.
pub fn compare_connections(
    profile: &WorkerProfile,
    broker: &PostgresConfig,
    limit: ServerConnectionLimit,
) -> ConnectionFit {
    let need = worker_connection_need(profile, broker);
    match need.total() > u64::from(limit.usable()) {
        true => ConnectionFit::ExceedsServer { need, limit },
        false => ConnectionFit::Fits { need, limit },
    }
}

/// The error of a profile whose worker needs more connections than the
/// server has usable slots.
fn exceeds_server_error(
    name: &str,
    need: &WorkerConnectionNeed,
    limit: ServerConnectionLimit,
) -> HorsiesError {
    HorsiesError::new(format!(
        "{}: {}",
        profile_path(name),
        exceeds_server_text(need, limit)
    ))
    .with_code(ErrorCode::CheckWorkerConnections)
    .with_note(need.formula())
    .with_note(need.task_sends_note())
    .with_help(CONNECTIONS_HELP)
}

/// One error for each profile whose worker needs more connections than the
/// server has usable slots, in name order (Python
/// `_worker_connection_errors`). Pure.
pub fn worker_connection_errors(
    profiles: &BTreeMap<String, WorkerProfile>,
    broker: &PostgresConfig,
    limit: ServerConnectionLimit,
) -> Vec<HorsiesError> {
    profiles
        .iter()
        .filter_map(
            |(name, profile)| match compare_connections(profile, broker, limit) {
                ConnectionFit::ExceedsServer { need, limit } => {
                    Some(exceeds_server_error(name, &need, limit))
                }
                ConnectionFit::Fits { .. } => None,
            },
        )
        .collect()
}

/// The error of a limit that was not read. Pure.
pub fn connection_limit_unreadable_error(cause: &ConnectionLimitUnreadable) -> HorsiesError {
    HorsiesError::new("the server connection limit could not be read")
        .with_code(ErrorCode::CheckWorkerConnections)
        .with_note(cause.to_string())
        .with_help(LIMIT_UNREADABLE_HELP)
}

/// Whether the live connection check reads the server limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LimitRead {
    /// No worker profile, or PgBouncer transaction mode: nothing is read.
    NotNeeded,
    /// At least one profile on a direct or session-pooled URL.
    Needed,
}

/// The rule of both clients: no profile to compare, or connections that go
/// through a transaction pooler, read nothing. Pure.
fn limit_read(profiles: &BTreeMap<String, WorkerProfile>, broker: &PostgresConfig) -> LimitRead {
    match (profiles.is_empty(), broker.pgbouncer_transaction_mode) {
        (true, _) | (_, true) => LimitRead::NotNeeded,
        (false, false) => LimitRead::Needed,
    }
}

/// Reads the limit once through `broker` and compares each profile with it:
/// one error per profile that does not fit, in name order, or one error
/// when the limit was not read. Writes nothing. Does not retry the read.
async fn read_and_compare(
    profiles: &BTreeMap<String, WorkerProfile>,
    broker_config: &PostgresConfig,
    broker: &PostgresBroker,
) -> Vec<HorsiesError> {
    match read_server_connection_limit(broker).await {
        Ok(limit) => worker_connection_errors(profiles, broker_config, limit),
        Err(cause) => vec![connection_limit_unreadable_error(&cause)],
    }
}

/// The live connection check of `horsies check --live` on a connected
/// broker (the Rust client's `check_live`; Python
/// `Horsies._check_worker_connections`).
///
/// - No worker profile, or PgBouncer transaction mode: reads nothing and
///   returns no error.
/// - Otherwise reads the limit once through `broker` and returns one error
///   per profile that does not fit, in name order, or one error when the
///   limit was not read.
///
/// Writes nothing. Does not retry the read.
pub async fn check_worker_connections(
    config: &AppConfig,
    broker: &PostgresBroker,
) -> Vec<HorsiesError> {
    match limit_read(&config.worker_profiles, &config.broker) {
        LimitRead::NotNeeded => Vec::new(),
        LimitRead::Needed => {
            read_and_compare(&config.worker_profiles, &config.broker, broker).await
        }
    }
}

/// The live connection check of `horsies check --live` on a lazy handle:
/// the profile set `profiles` against the database of `handle`'s manifest
/// (the Python client's `Horsies._check_worker_connections`, through the
/// bridge). The same rule as [`check_worker_connections`]:
///
/// - No worker profile, or PgBouncer transaction mode in the manifest:
///   connects nothing, reads nothing, returns no error.
/// - Otherwise connects the handle's broker (a connect that fails is the
///   one error of an unreadable limit), reads the limit once, and returns
///   one error per profile that does not fit, in name order, or one error
///   when the limit was not read.
///
/// Writes nothing; installs no schema. Does not retry the connect or the
/// read. Every error has the code `CHECK_WORKER_CONNECTIONS`.
pub async fn check_handle_worker_connections(
    profiles: &BTreeMap<String, WorkerProfile>,
    handle: &CoreHandle,
) -> Vec<HorsiesError> {
    let manifest = handle.manifest();
    let broker_config = manifest
        .database
        .to_postgres_config(manifest.send.retain_rerun_input_default);
    match limit_read(profiles, &broker_config) {
        LimitRead::NotNeeded => Vec::new(),
        LimitRead::Needed => match handle.broker().await {
            Ok(broker) => read_and_compare(profiles, &broker_config, &broker).await,
            Err(error) => vec![connection_limit_unreadable_error(
                &ConnectionLimitUnreadable::Database(error),
            )],
        },
    }
}

/// The connection fit of the profile that a worker runs, read at boot before
/// the worker starts. The worker starts in every case (Python
/// `_log_connection_fit`, `python/horsies/core/cli.py:711-745`).
#[derive(Debug)]
pub enum BootConnectionFit {
    /// PgBouncer transaction mode: nothing is read.
    NotComparedThroughPooler,
    /// The limit was not read; the profile is not compared.
    LimitUnreadable(ConnectionLimitUnreadable),
    /// The limit was read and the profile compared with it.
    Compared(ConnectionFit),
}

/// Reads the server limit through `broker` and compares the profile with it,
/// unless the pooled connections go through a transaction pooler. Writes
/// nothing; never refuses: an unreadable limit is an outcome.
pub async fn boot_connection_fit(
    profile: &WorkerProfile,
    broker_config: &PostgresConfig,
    broker: &PostgresBroker,
) -> BootConnectionFit {
    match broker_config.pgbouncer_transaction_mode {
        true => BootConnectionFit::NotComparedThroughPooler,
        false => match read_server_connection_limit(broker).await {
            Ok(limit) => {
                BootConnectionFit::Compared(compare_connections(profile, broker_config, limit))
            }
            Err(cause) => BootConnectionFit::LimitUnreadable(cause),
        },
    }
}

/// The level of a boot log record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootLogLevel {
    Info,
    Warning,
}

/// The log record of a boot connection fit: its level and its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootConnectionLog {
    pub level: BootLogLevel,
    pub message: String,
}

/// The record that the worker logs for the fit of its profile `name`, with
/// Python's level and text (`python/horsies/core/cli.py:691-745`): a profile above
/// the usable slots or an unreadable limit is a warning; a fitting profile
/// and a transaction pooler are info. Pure.
pub fn boot_connection_log(name: &str, fit: &BootConnectionFit) -> BootConnectionLog {
    let profile = quoted_profile_name(name);
    match fit {
        BootConnectionFit::NotComparedThroughPooler => BootConnectionLog {
            level: BootLogLevel::Info,
            message: format!(
                "Worker profile {profile}: connections go through a transaction pooler; the \
                 server connection limit is not compared"
            ),
        },
        BootConnectionFit::LimitUnreadable(cause) => BootConnectionLog {
            level: BootLogLevel::Warning,
            message: format!(
                "Worker profile {profile}: the server connection limit could not be read \
                 ({cause}); the connections of this worker were not compared with it"
            ),
        },
        BootConnectionFit::Compared(fit @ ConnectionFit::Fits { .. }) => BootConnectionLog {
            level: BootLogLevel::Info,
            message: format!("Worker profile {profile}: {}", fit.describe()),
        },
        BootConnectionFit::Compared(fit @ ConnectionFit::ExceedsServer { need, .. }) => {
            BootConnectionLog {
                level: BootLogLevel::Warning,
                message: format!(
                    "Worker profile {profile}: {}. {}; {}. To fix: {CONNECTIONS_HELP}",
                    fit.describe(),
                    need.formula(),
                    need.task_sends_note()
                ),
            }
        }
    }
}

/// Writes the record to the log at its level.
pub fn log_boot_connection_fit(record: &BootConnectionLog) {
    match record.level {
        BootLogLevel::Info => tracing::info!("{}", record.message),
        BootLogLevel::Warning => tracing::warn!("{}", record.message),
    }
}

#[cfg(test)]
mod tests;

//! The broker's core pools: one [`CorePool`] per role, built from a
//! [`PostgresConfig`].
//!
//! - The main URL (`database_url`) has one pool. A producer gets a
//!   [`PoolRole::Main`] pool with no reserve. A worker gets a
//!   [`PoolRole::WorkerProfile`] pool that keeps
//!   [`WORKER_RESERVED_CONNECTIONS`] free from batch checkouts, for the reaper
//!   and the per-task writes.
//! - The session URL has its own [`PoolRole::Session`] pool when it differs
//!   from the main URL; otherwise session work uses the main pool.
//! - Every pool: `pool_size + max_overflow` connections, `pool_timeout` as the
//!   checkout wait, `pool_recycle` as the connection age limit, the 1 s idle
//!   ping when `pool_pre_ping` is on, and the TCP keepalives and TCP user
//!   timeout of the config ([`PostgresConfig::socket_settings`]) on each
//!   connection. The main pool sends unnamed statements only in PgBouncer
//!   transaction mode; the session pool always keeps its session.
//! - A child of a worker ([`PoolProfile::Child`]) gets the pool of Python's
//!   child (`clients/python/horsies/core/worker/child_pool.py:122-136`): its main URL pool
//!   keeps `min_size` connections open, and with `check` it pings every
//!   pooled connection at checkout in place of the 1 s idle ping.
//!
//! Building the pools connects to nothing: a connection opens at the first
//! checkout. A child's main pool that keeps connections open starts the task
//! that opens them.
//!
//! [`CoreHandle`] holds the pools between `Idle` and `Open`: an operation
//! opens them when the handle is `Idle`, and [`CoreHandle::close`] returns the
//! handle to `Idle` after the writes in flight end (bounded by
//! [`CLOSE_WRITE_BOUND`]).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::task::{task_tracker::TaskTrackerToken, TaskTracker};

use crate::core::PostgresConfig;
use crate::db::{
    parse_connection_url, ConnectPlan, ConnectPlanError, ConnectionAge, ConnectionEnvironment,
    CorePool, EnvironmentError, IdlePing, KeptOpen, MaskedUrl, PoolPlan, PoolRole, PoolSizing,
    PoolSizingError, Reserve, SocketSettings, StatementProtocol, UrlError, DEFAULT_IDLE_PING_AFTER,
    WORKER_RESERVED_CONNECTIONS,
};
use crate::manifest::PoolRecycle;

/// The longest [`CoreHandle::close`] waits for the writes in flight
/// (Python's `CLOSE_HANDOFF_TIMEOUT`).
pub const CLOSE_WRITE_BOUND: Duration = Duration::from_secs(5);

/// The process a broker's pools serve.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoolProfile {
    /// A producer or a monitoring handle: the main URL pool keeps no reserve.
    Producer,
    /// A worker parent: the main URL pool is the worker profile pool, which
    /// keeps [`WORKER_RESERVED_CONNECTIONS`] free from batch checkouts.
    Worker,
    /// A child process of a worker: the main URL pool keeps no reserve,
    /// keeps `kept_open` connections open (Python `min_size`), and with
    /// `check` pings every pooled connection at checkout
    /// ([`IdlePing::EveryCheckout`], Python
    /// `check=ConnectionPool.check_connection`); without `check` it pings
    /// none.
    Child { kept_open: KeptOpen, check: bool },
}

/// The pool that session work (schema work, partition maintenance) uses.
pub(crate) enum SessionPool {
    /// `session_database_url` is absent or equal to `database_url`: session
    /// work uses the main pool.
    SameAsMain,
    /// A pool of its own on `session_database_url`.
    Separate {
        pool: CorePool,
        masked_url: MaskedUrl,
    },
}

/// The core pools of one broker.
pub(crate) struct BrokerPools {
    main: CorePool,
    main_url: MaskedUrl,
    session: SessionPool,
}

/// The connection settings of a broker pool were refused before any
/// connection opened. The same settings are refused again.
#[derive(Debug, thiserror::Error)]
pub enum PoolSettingsError {
    /// `PGPASSWORD` could not be read.
    #[error("reading the connection environment failed: {0}")]
    Environment(EnvironmentError),
    /// The statement layer refused the URL of a pool.
    #[error("the URL of the {role} pool is refused: {cause}")]
    Url { role: PoolRole, cause: UrlError },
    /// A TLS file, `~/.pgpass` or the process user name could not be read.
    #[error("the connection plan of the {role} pool could not be loaded: {cause}")]
    Plan {
        role: PoolRole,
        cause: ConnectPlanError,
    },
    /// `pool_size + max_overflow` cannot carry the pool's role.
    #[error("the size of the {role} pool is refused: {cause}")]
    Sizing {
        role: PoolRole,
        cause: PoolSizingError,
    },
}

impl BrokerPools {
    /// Builds the pools of `config` for `profile` with an environment the
    /// caller read.
    ///
    /// Reads, per URL, the TLS files the URL names, `~/.pgpass` when no
    /// password is given, and the process user name when the URL names no
    /// user. Connects to nothing, but for a [`PoolProfile::Child`] main pool
    /// that keeps connections open: it starts the task that opens them, so it
    /// must run inside a tokio runtime. Every pool takes the socket settings
    /// of `config`.
    ///
    /// Errors: [`PoolSettingsError`]; nothing was built.
    pub(crate) fn open_with_environment(
        config: &PostgresConfig,
        profile: PoolProfile,
        environment: &ConnectionEnvironment,
    ) -> Result<BrokerPools, PoolSettingsError> {
        let socket = config.socket_settings();
        let main_shape = main_pool_shape(profile);
        let (main, main_url) = open_pool(
            &config.database_url,
            main_pool_plan(config, profile).map_err(|cause| PoolSettingsError::Sizing {
                role: main_shape.role,
                cause,
            })?,
            environment,
            &socket,
        )?;
        let session_url = config.effective_session_database_url();
        let session = match session_url == config.database_url {
            true => SessionPool::SameAsMain,
            false => {
                let (pool, masked_url) = open_pool(
                    session_url,
                    pool_plan(
                        config,
                        PoolRole::Session,
                        Reserve::NoReserve,
                        StatementProtocol::PreparedPerConnection,
                    )
                    .map_err(|cause| PoolSettingsError::Sizing {
                        role: PoolRole::Session,
                        cause,
                    })?,
                    environment,
                    &socket,
                )?;
                SessionPool::Separate { pool, masked_url }
            }
        };
        Ok(BrokerPools {
            main,
            main_url,
            session,
        })
    }

    /// The pool on `database_url`.
    pub(crate) fn main(&self) -> &CorePool {
        &self.main
    }

    /// The pool for session work.
    pub(crate) fn session(&self) -> &SessionPool {
        &self.session
    }

    /// The pool that session work checks out from: the session pool, or the
    /// main pool when both URLs are the same. Either keeps its session: the
    /// config refuses PgBouncer transaction mode without a separate session
    /// URL.
    pub(crate) fn session_work(&self) -> &CorePool {
        match &self.session {
            SessionPool::SameAsMain => &self.main,
            SessionPool::Separate { pool, .. } => pool,
        }
    }

    /// The session URL with its password masked (the main URL when both are
    /// the same).
    pub(crate) fn session_masked_url(&self) -> &MaskedUrl {
        match &self.session {
            SessionPool::SameAsMain => &self.main_url,
            SessionPool::Separate { masked_url, .. } => masked_url,
        }
    }

    /// Closes every pool: later checkouts fail with `PoolClosed`. Checked-out
    /// connections end when their guards drop.
    pub(crate) fn close(&self) {
        self.main.close();
        match &self.session {
            SessionPool::SameAsMain => {}
            SessionPool::Separate { pool, .. } => pool.close(),
        }
    }
}

/// The pools of an open handle and the writes that run on them.
pub(crate) struct OpenPools {
    pools: BrokerPools,
    writes: TaskTracker,
}

impl OpenPools {
    pub(crate) fn pools(&self) -> &BrokerPools {
        &self.pools
    }

    pub(crate) fn main(&self) -> &CorePool {
        self.pools.main()
    }
}

/// A write in flight on an open handle. [`CoreHandle::close`] waits for it,
/// up to [`CLOSE_WRITE_BOUND`]. Dropping it ends the write.
pub(crate) struct WriteInFlight {
    _token: TaskTrackerToken,
}

/// The stage of a broker's core pools.
enum HandleState {
    /// No pools: none built yet, or closed. The next operation opens them.
    Idle,
    /// Pools built; operations run on them.
    Open(Arc<OpenPools>),
}

/// The core pools of one broker and their stage.
///
/// The handle keeps what it needs to open the pools again after a close: the
/// config, the profile and the environment read at construction.
pub(crate) struct CoreHandle {
    config: PostgresConfig,
    profile: PoolProfile,
    environment: ConnectionEnvironment,
    state: Mutex<HandleState>,
}

/// What a close found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CloseOutcome {
    /// The handle was `Idle`: nothing to close.
    AlreadyIdle,
    /// Every write in flight ended within the bound; the pools are closed.
    Closed,
    /// `unfinished` writes did not end within [`CLOSE_WRITE_BOUND`]; their
    /// outcome is unknown. The pools are closed.
    ClosedWithWritesUnfinished { unfinished: usize },
}

impl CoreHandle {
    /// Builds the pools at once, so refused settings fail the constructor
    /// before any connection. Reads what
    /// [`BrokerPools::open_with_environment`] reads.
    pub(crate) fn open_with_environment(
        config: &PostgresConfig,
        profile: PoolProfile,
        environment: ConnectionEnvironment,
    ) -> Result<CoreHandle, PoolSettingsError> {
        let pools = BrokerPools::open_with_environment(config, profile, &environment)?;
        Ok(CoreHandle {
            config: config.clone(),
            profile,
            environment,
            state: Mutex::new(HandleState::Open(Arc::new(OpenPools {
                pools,
                writes: TaskTracker::new(),
            }))),
        })
    }

    /// Builds the pools with the process environment.
    pub(crate) fn open(
        config: &PostgresConfig,
        profile: PoolProfile,
    ) -> Result<CoreHandle, PoolSettingsError> {
        let environment =
            ConnectionEnvironment::capture().map_err(PoolSettingsError::Environment)?;
        CoreHandle::open_with_environment(config, profile, environment)
    }

    /// The config that the pools are built from.
    pub(crate) fn config(&self) -> &PostgresConfig {
        &self.config
    }

    /// The open pools, for a read. An `Idle` handle builds its pools first
    /// (no connection; the TLS files and `~/.pgpass` are read again).
    ///
    /// Errors: [`PoolSettingsError`] when the settings are refused at the
    /// reopen; the handle stays `Idle`.
    pub(crate) fn pools(&self) -> Result<Arc<OpenPools>, PoolSettingsError> {
        let mut state = self.lock_state();
        self.open_in(&mut *state)
    }

    /// The open pools for a write, and the token that keeps the write in
    /// the close's wait. The token is taken under the state lock, so a close
    /// that swaps the pools out after it waits for this write. Errors as
    /// [`CoreHandle::pools`].
    pub(crate) fn pools_for_write(
        &self,
    ) -> Result<(Arc<OpenPools>, WriteInFlight), PoolSettingsError> {
        let mut state = self.lock_state();
        let open = self.open_in(&mut *state)?;
        let write = WriteInFlight {
            _token: open.writes.token(),
        };
        Ok((open, write))
    }

    /// The pools of an `Open` state; an `Idle` state builds them and becomes
    /// `Open`.
    fn open_in(&self, state: &mut HandleState) -> Result<Arc<OpenPools>, PoolSettingsError> {
        match state {
            HandleState::Open(open) => Ok(Arc::clone(open)),
            HandleState::Idle => {
                let pools = BrokerPools::open_with_environment(
                    &self.config,
                    self.profile,
                    &self.environment,
                )?;
                let open = Arc::new(OpenPools {
                    pools,
                    writes: TaskTracker::new(),
                });
                *state = HandleState::Open(Arc::clone(&open));
                Ok(open)
            }
        }
    }

    /// Closes the open pools and returns the handle to `Idle`.
    ///
    /// No new operation takes the old pools: the next one opens new pools.
    /// Writes in flight run to their end within [`CLOSE_WRITE_BOUND`]; then
    /// the old pools close, so a read in flight that checks out again fails
    /// with `PoolClosed`. Connections checked out at that moment end when
    /// their guards drop.
    pub(crate) async fn close(&self) -> CloseOutcome {
        let previous = std::mem::replace(&mut *self.lock_state(), HandleState::Idle);
        let open = match previous {
            HandleState::Idle => return CloseOutcome::AlreadyIdle,
            HandleState::Open(open) => open,
        };
        let _closed_now = open.writes.close();
        let outcome = match tokio::time::timeout(CLOSE_WRITE_BOUND, open.writes.wait()).await {
            Ok(()) => CloseOutcome::Closed,
            Err(_elapsed) => CloseOutcome::ClosedWithWritesUnfinished {
                unfinished: open.writes.len(),
            },
        };
        open.pools.close();
        outcome
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, HandleState> {
        self.state
            .lock()
            .expect("the core handle state lock is held only for a state swap or a pool build")
    }
}

/// The role and reserve of the main URL pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MainPoolShape {
    role: PoolRole,
    reserve: Reserve,
}

fn main_pool_shape(profile: PoolProfile) -> MainPoolShape {
    match profile {
        PoolProfile::Producer => MainPoolShape {
            role: PoolRole::Main,
            reserve: Reserve::NoReserve,
        },
        PoolProfile::Worker => MainPoolShape {
            role: PoolRole::WorkerProfile,
            reserve: Reserve::KeepFree(WORKER_RESERVED_CONNECTIONS),
        },
        PoolProfile::Child { .. } => MainPoolShape {
            role: PoolRole::Main,
            reserve: Reserve::NoReserve,
        },
    }
}

/// The plan of the main URL pool of `profile`. Pure.
///
/// Errors: [`PoolSizingError`] as [`pool_plan`], and
/// [`PoolSizingError::KeptOpenAboveSize`] for a child that keeps more
/// connections open than its pool holds.
fn main_pool_plan(
    config: &PostgresConfig,
    profile: PoolProfile,
) -> Result<PoolPlan, PoolSizingError> {
    let shape = main_pool_shape(profile);
    let plan = pool_plan(config, shape.role, shape.reserve, runtime_protocol(config))?;
    match profile {
        PoolProfile::Producer | PoolProfile::Worker => Ok(plan),
        PoolProfile::Child { kept_open, check } => Ok(PoolPlan {
            sizing: plan.sizing.keeping_open(kept_open)?,
            idle_ping: match check {
                true => IdlePing::EveryCheckout,
                false => IdlePing::Off,
            },
            ..plan
        }),
    }
}

/// The statement protocol of the main URL: unnamed statements only behind
/// PgBouncer transaction pooling.
fn runtime_protocol(config: &PostgresConfig) -> StatementProtocol {
    match config.pgbouncer_transaction_mode {
        true => StatementProtocol::UnnamedOnly,
        false => StatementProtocol::PreparedPerConnection,
    }
}

/// The plan of one pool from the config's pool settings. Pure.
///
/// Errors: [`PoolSizingError`] when `pool_size + max_overflow` is zero, above
/// PostgreSQL's limit, or not above the reserve.
pub(crate) fn pool_plan(
    config: &PostgresConfig,
    role: PoolRole,
    reserve: Reserve,
    protocol: StatementProtocol,
) -> Result<PoolPlan, PoolSizingError> {
    let size = config.pool_size.saturating_add(config.max_overflow);
    Ok(PoolPlan {
        role,
        sizing: PoolSizing::new(size, reserve)?,
        checkout_wait: Duration::from_secs(u64::from(config.pool_timeout)),
        idle_ping: match config.pool_pre_ping {
            true => IdlePing::AfterIdle(DEFAULT_IDLE_PING_AFTER),
            false => IdlePing::Off,
        },
        max_age: connection_age(config.pool_recycle),
        protocol,
    })
}

/// The age at which a pool replaces a connection: never, or after the
/// recycle seconds. Pure.
fn connection_age(recycle: PoolRecycle) -> ConnectionAge {
    match recycle {
        PoolRecycle::Never => ConnectionAge::Unlimited,
        PoolRecycle::AfterSeconds { seconds } => {
            ConnectionAge::ReplaceAfter(Duration::from_secs(u64::from(seconds.get())))
        }
    }
}

/// Parses one URL, loads its connection plan with `socket` and builds the
/// pool. Returns the pool and its URL with the password masked.
fn open_pool(
    url: &str,
    plan: PoolPlan,
    environment: &ConnectionEnvironment,
    socket: &SocketSettings,
) -> Result<(CorePool, MaskedUrl), PoolSettingsError> {
    let role = plan.role;
    let settings = parse_connection_url(url, environment)
        .map_err(|cause| PoolSettingsError::Url { role, cause })?;
    let connect = ConnectPlan::load(&settings, environment, socket)
        .map_err(|cause| PoolSettingsError::Plan { role, cause })?;
    Ok((
        CorePool::new(plan, Arc::new(connect)),
        settings.masked_url().clone(),
    ))
}

/// The core handle (producer pools) on the test database of `url`, for
/// brokers that tests build from the URL. The environment is isolated: test
/// URLs carry their password.
#[cfg(test)]
pub(crate) fn handle_for_test_url(url: &str) -> CoreHandle {
    CoreHandle::open_with_environment(
        &PostgresConfig::from_url(url),
        PoolProfile::Producer,
        ConnectionEnvironment::isolated(),
    )
    .expect("the URL of a test database is accepted by the statement layer")
}

/// A main core pool on the test database of `url`, for the free lifecycle
/// writers that tests call with a pool of their own. The same pool as the
/// main pool of [`handle_for_test_url`]; connects to nothing until the first
/// checkout.
#[cfg(test)]
pub(crate) fn core_pool_for_test_url(url: &str) -> CorePool {
    let config = PostgresConfig::from_url(url);
    let plan = pool_plan(
        &config,
        PoolRole::Main,
        Reserve::NoReserve,
        runtime_protocol(&config),
    )
    .expect("the size of a test pool is accepted by the statement layer");
    let (core_pool, _masked_url) = open_pool(
        &config.database_url,
        plan,
        &ConnectionEnvironment::isolated(),
        &config.socket_settings(),
    )
    .expect("the URL of a test pool is accepted by the statement layer");
    core_pool
}

/// A worker profile core pool of `size` connections on the test database of
/// `url`: it keeps [`WORKER_RESERVED_CONNECTIONS`] free from batch
/// checkouts, as the pool of a worker. Connects to nothing until the first
/// checkout.
#[cfg(test)]
pub(crate) fn worker_core_pool_for_test_url(url: &str, size: u32) -> CorePool {
    let mut config = PostgresConfig::from_url(url);
    config.pool_size = size;
    config.max_overflow = 0;
    let plan = pool_plan(
        &config,
        PoolRole::WorkerProfile,
        Reserve::KeepFree(WORKER_RESERVED_CONNECTIONS),
        runtime_protocol(&config),
    )
    .expect("a test worker pool is larger than its reserve");
    let (core_pool, _masked_url) = open_pool(
        &config.database_url,
        plan,
        &ConnectionEnvironment::isolated(),
        &config.socket_settings(),
    )
    .expect("the URL of a test pool is accepted by the statement layer");
    core_pool
}

/// A session pool of `size` connections on the test database of `url`: no
/// reserve, statements prepared per connection, as the separate session pool
/// of a broker. Connects to nothing until the first checkout.
#[cfg(test)]
pub(crate) fn session_core_pool_for_test_url(url: &str, size: u32) -> CorePool {
    let mut config = PostgresConfig::from_url(url);
    config.pool_size = size;
    config.max_overflow = 0;
    let plan = pool_plan(
        &config,
        PoolRole::Session,
        Reserve::NoReserve,
        StatementProtocol::PreparedPerConnection,
    )
    .expect("a test session pool has at least one connection");
    let (core_pool, _masked_url) = open_pool(
        &config.database_url,
        plan,
        &ConnectionEnvironment::isolated(),
        &config.socket_settings(),
    )
    .expect("the URL of a test pool is accepted by the statement layer");
    core_pool
}

/// The checkout wait of [`unpinged_single_connection_core_pool_for_url`].
#[cfg(test)]
const TEST_CHECKOUT_WAIT: Duration = Duration::from_secs(30);

/// A core pool of one connection on the test database of `url` that never
/// pings a pooled connection at checkout (Python's worker pre-ping off) and
/// never replaces it for its age, with the default socket settings. Once a
/// statement is prepared on the connection, the statement is its only round
/// trip. Connects to nothing until the first checkout.
#[cfg(test)]
pub(crate) fn unpinged_single_connection_core_pool_for_url(url: &str) -> CorePool {
    let plan = PoolPlan {
        role: PoolRole::Main,
        sizing: PoolSizing::new(1, Reserve::NoReserve)
            .expect("one connection with no reserve is a valid size"),
        checkout_wait: TEST_CHECKOUT_WAIT,
        idle_ping: IdlePing::Off,
        max_age: ConnectionAge::Unlimited,
        protocol: StatementProtocol::PreparedPerConnection,
    };
    let (core_pool, _masked_url) = open_pool(
        url,
        plan,
        &ConnectionEnvironment::isolated(),
        &crate::db::DEFAULT_SOCKET_SETTINGS,
    )
    .expect("the URL of a test pool is accepted by the statement layer");
    core_pool
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::core::config::socket::{
        KeepaliveProbes, KeepaliveSeconds, TcpKeepalivesConfig, TcpUserTimeoutConfig,
        TcpUserTimeoutMs,
    };
    use crate::db::{CheckoutClass, CheckoutFailure};

    fn config() -> PostgresConfig {
        let mut config =
            PostgresConfig::from_url("postgresql://horsies:secret@db.example:5432/app");
        config.pool_size = 5;
        config.max_overflow = 3;
        config.pool_timeout = 7;
        config.pool_recycle = PoolRecycle::after_seconds(1800);
        config.pool_pre_ping = true;
        config
    }

    #[test]
    fn a_pool_takes_size_wait_age_and_ping_from_the_config() {
        let plan = pool_plan(
            &config(),
            PoolRole::Main,
            Reserve::NoReserve,
            StatementProtocol::PreparedPerConnection,
        )
        .expect("valid plan");
        assert_eq!(plan.role, PoolRole::Main);
        assert_eq!(plan.sizing.size(), 8);
        assert_eq!(plan.sizing.batch_limit(), 8);
        assert_eq!(plan.checkout_wait, Duration::from_secs(7));
        assert_eq!(
            plan.max_age,
            ConnectionAge::ReplaceAfter(Duration::from_secs(1800))
        );
        assert_eq!(plan.idle_ping, IdlePing::AfterIdle(DEFAULT_IDLE_PING_AFTER));
        assert_eq!(plan.protocol, StatementProtocol::PreparedPerConnection);
    }

    #[test]
    fn a_pool_that_never_recycles_keeps_its_connections() {
        let mut config = config();
        config.pool_recycle = PoolRecycle::Never;
        let plan = pool_plan(
            &config,
            PoolRole::Main,
            Reserve::NoReserve,
            StatementProtocol::PreparedPerConnection,
        )
        .expect("valid plan");
        assert_eq!(plan.max_age, ConnectionAge::Unlimited);
    }

    #[test]
    fn pre_ping_off_turns_the_idle_ping_off() {
        let mut config = config();
        config.pool_pre_ping = false;
        let plan = pool_plan(
            &config,
            PoolRole::Main,
            Reserve::NoReserve,
            StatementProtocol::PreparedPerConnection,
        )
        .expect("valid plan");
        assert_eq!(plan.idle_ping, IdlePing::Off);
    }

    #[test]
    fn a_worker_main_pool_keeps_the_reserve() {
        let shape = main_pool_shape(PoolProfile::Worker);
        assert_eq!(shape.role, PoolRole::WorkerProfile);
        assert_eq!(
            shape.reserve,
            Reserve::KeepFree(WORKER_RESERVED_CONNECTIONS)
        );
        let plan = pool_plan(
            &config(),
            shape.role,
            shape.reserve,
            StatementProtocol::PreparedPerConnection,
        )
        .expect("valid plan");
        assert_eq!(plan.sizing.batch_limit(), 6);
    }

    #[test]
    fn a_producer_main_pool_keeps_no_reserve() {
        let shape = main_pool_shape(PoolProfile::Producer);
        assert_eq!(shape.role, PoolRole::Main);
        assert_eq!(shape.reserve, Reserve::NoReserve);
    }

    fn kept(count: u32) -> KeptOpen {
        KeptOpen::Connections(NonZeroU32::new(count).expect("a kept count above 0"))
    }

    /// Python `_initialize_worker_pool(min_size=2, check_on_checkout=True)`:
    /// two connections stay open, and each checkout checks its connection.
    #[test]
    fn a_child_pool_keeps_its_minimum_open_and_checks_each_checkout() {
        let plan = main_pool_plan(
            &config(),
            PoolProfile::Child {
                kept_open: kept(2),
                check: true,
            },
        )
        .expect("valid plan");
        assert_eq!(plan.role, PoolRole::Main);
        assert_eq!(plan.sizing.size(), 8);
        assert_eq!(plan.sizing.reserve(), Reserve::NoReserve);
        assert_eq!(plan.sizing.kept_open(), kept(2));
        assert_eq!(plan.idle_ping, IdlePing::EveryCheckout);
        assert_eq!(plan.checkout_wait, Duration::from_secs(7));
        assert_eq!(
            plan.max_age,
            ConnectionAge::ReplaceAfter(Duration::from_secs(1800))
        );
    }

    /// Python `check_on_checkout=False`: no check, also when the config's
    /// `pool_pre_ping` is on.
    #[test]
    fn a_child_pool_without_check_pings_no_connection() {
        let plan = main_pool_plan(
            &config(),
            PoolProfile::Child {
                kept_open: KeptOpen::NoConnections,
                check: false,
            },
        )
        .expect("valid plan");
        assert_eq!(plan.idle_ping, IdlePing::Off);
        assert_eq!(plan.sizing.kept_open(), KeptOpen::NoConnections);
    }

    /// Python `ValueError('min_size must be <= max_size')`.
    #[test]
    fn a_child_pool_that_keeps_more_than_its_size_open_is_refused() {
        let refused = BrokerPools::open_with_environment(
            &config(),
            PoolProfile::Child {
                kept_open: kept(9),
                check: false,
            },
            &ConnectionEnvironment::isolated(),
        );
        assert!(matches!(
            refused,
            Err(PoolSettingsError::Sizing {
                role: PoolRole::Main,
                cause: PoolSizingError::KeptOpenAboveSize { size: 8, kept: 9 },
            })
        ));
    }

    #[test]
    fn producer_and_worker_pools_keep_no_connection_open() {
        for profile in [PoolProfile::Producer, PoolProfile::Worker] {
            let plan = main_pool_plan(&config(), profile).expect("valid plan");
            assert_eq!(plan.sizing.kept_open(), KeptOpen::NoConnections);
            assert_eq!(plan.idle_ping, IdlePing::AfterIdle(DEFAULT_IDLE_PING_AFTER));
        }
    }

    #[test]
    fn a_worker_pool_that_cannot_keep_the_reserve_is_refused() {
        let mut config = config();
        config.pool_size = 2;
        config.max_overflow = 0;
        let shape = main_pool_shape(PoolProfile::Worker);
        assert_eq!(
            pool_plan(
                &config,
                shape.role,
                shape.reserve,
                StatementProtocol::PreparedPerConnection
            )
            .map(|plan| plan.sizing),
            Err(PoolSizingError::ReserveNotBelowSize {
                size: 2,
                reserve: 2
            }),
        );
    }

    #[test]
    fn a_zero_pool_is_refused() {
        let mut config = config();
        config.pool_size = 0;
        config.max_overflow = 0;
        assert_eq!(
            pool_plan(
                &config,
                PoolRole::Main,
                Reserve::NoReserve,
                StatementProtocol::PreparedPerConnection
            )
            .map(|plan| plan.sizing),
            Err(PoolSizingError::SizeZero),
        );
    }

    #[test]
    fn transaction_mode_sends_unnamed_statements_only() {
        let mut config = config();
        assert_eq!(
            runtime_protocol(&config),
            StatementProtocol::PreparedPerConnection
        );
        config.pgbouncer_transaction_mode = true;
        assert_eq!(runtime_protocol(&config), StatementProtocol::UnnamedOnly);
    }

    #[test]
    fn equal_urls_share_the_main_pool() {
        let pools = BrokerPools::open_with_environment(
            &config(),
            PoolProfile::Producer,
            &ConnectionEnvironment::isolated(),
        )
        .expect("pools build without I/O");
        assert!(matches!(pools.session, SessionPool::SameAsMain));
        assert_eq!(pools.main().role(), PoolRole::Main);
    }

    #[test]
    fn session_work_uses_the_main_pool_when_the_urls_are_equal() {
        let pools = BrokerPools::open_with_environment(
            &config(),
            PoolProfile::Worker,
            &ConnectionEnvironment::isolated(),
        )
        .expect("pools build without I/O");
        assert_eq!(pools.session_work().role(), PoolRole::WorkerProfile);

        let mut separate = config();
        separate.session_database_url =
            Some("postgresql://horsies:secret@direct.example:5432/app".to_owned());
        let pools = BrokerPools::open_with_environment(
            &separate,
            PoolProfile::Worker,
            &ConnectionEnvironment::isolated(),
        )
        .expect("pools build without I/O");
        assert_eq!(pools.session_work().role(), PoolRole::Session);
    }

    #[test]
    fn a_distinct_session_url_gets_its_own_pool() {
        let mut config = config();
        config.session_database_url =
            Some("postgresql://horsies:secret@direct.example:5432/app".to_owned());
        let pools = BrokerPools::open_with_environment(
            &config,
            PoolProfile::Worker,
            &ConnectionEnvironment::isolated(),
        )
        .expect("pools build without I/O");
        assert_eq!(pools.main().role(), PoolRole::WorkerProfile);
        assert!(matches!(
            pools.session(),
            SessionPool::Separate { pool, .. } if pool.role() == PoolRole::Session
        ));
        assert_eq!(
            pools.session_masked_url().as_str(),
            "postgresql://horsies:***@direct.example:5432/app"
        );
    }

    #[test]
    fn a_url_the_statement_layer_refuses_names_its_pool() {
        let mut config = config();
        config.session_database_url =
            Some("postgresql://horsies@direct.example/app?keepalives=0".to_owned());
        let refused = BrokerPools::open_with_environment(
            &config,
            PoolProfile::Producer,
            &ConnectionEnvironment::isolated(),
        );
        assert!(matches!(
            refused,
            Err(PoolSettingsError::Url {
                role: PoolRole::Session,
                ..
            })
        ));
    }

    /// [`config`] with a distinct session URL, so the broker has a main and a
    /// session pool, and the given TCP settings.
    fn config_with_socket(
        tcp_keepalives: TcpKeepalivesConfig,
        tcp_user_timeout: TcpUserTimeoutConfig,
    ) -> PostgresConfig {
        let mut config = config();
        config.session_database_url =
            Some("postgresql://horsies:secret@direct.example:5432/app".to_owned());
        config.tcp_keepalives = tcp_keepalives;
        config.tcp_user_timeout = tcp_user_timeout;
        config
    }

    /// The pools of `config` for each profile.
    fn pools_of_each_profile(config: &PostgresConfig) -> [BrokerPools; 2] {
        [PoolProfile::Producer, PoolProfile::Worker].map(|profile| {
            BrokerPools::open_with_environment(config, profile, &ConnectionEnvironment::isolated())
                .expect("pools build without I/O")
        })
    }

    /// The main pool and the session pool of `pools`.
    fn main_and_session(pools: &BrokerPools) -> [&CorePool; 2] {
        let SessionPool::Separate { pool: session, .. } = pools.session() else {
            panic!("a distinct session URL gets its own pool");
        };
        [pools.main(), session]
    }

    #[test]
    fn keepalives_off_and_the_system_user_timeout_reach_every_pool() {
        let config = config_with_socket(
            TcpKeepalivesConfig::Off,
            TcpUserTimeoutConfig::SystemDefault,
        );
        for pools in pools_of_each_profile(&config) {
            for pool in main_and_session(&pools) {
                let driver = pool.connect_plan().first_attempt();
                let role = pool.role();
                assert!(!driver.get_keepalives(), "{role} pool");
                assert_eq!(driver.get_tcp_user_timeout(), None, "{role} pool");
            }
        }
    }

    #[test]
    fn keepalive_values_and_a_user_timeout_reach_every_pool() {
        let config = config_with_socket(
            TcpKeepalivesConfig::On {
                idle_seconds: KeepaliveSeconds::try_from(45_u32).expect("in bounds"),
                interval_seconds: KeepaliveSeconds::try_from(7_u32).expect("in bounds"),
                probes: KeepaliveProbes::try_from(5_u32).expect("in bounds"),
            },
            TcpUserTimeoutConfig::After {
                milliseconds: TcpUserTimeoutMs::try_from(1500_u32).expect("in bounds"),
            },
        );
        for pools in pools_of_each_profile(&config) {
            for pool in main_and_session(&pools) {
                let driver = pool.connect_plan().first_attempt();
                let role = pool.role();
                assert!(driver.get_keepalives(), "{role} pool");
                assert_eq!(
                    driver.get_keepalives_idle(),
                    Duration::from_secs(45),
                    "{role} pool"
                );
                assert_eq!(
                    driver.get_keepalives_interval(),
                    Some(Duration::from_secs(7)),
                    "{role} pool"
                );
                assert_eq!(driver.get_keepalives_retries(), Some(5), "{role} pool");
                assert_eq!(
                    driver.get_tcp_user_timeout(),
                    Some(&Duration::from_millis(1500)),
                    "{role} pool"
                );
            }
        }
    }

    fn open_handle() -> CoreHandle {
        CoreHandle::open_with_environment(
            &config(),
            PoolProfile::Producer,
            ConnectionEnvironment::isolated(),
        )
        .expect("a handle builds without I/O")
    }

    /// Python's close contract: close, then the next operation opens again.
    #[tokio::test]
    async fn a_closed_handle_opens_new_pools_at_the_next_operation() {
        let handle = open_handle();
        let first = handle.pools().expect("open pools");

        assert_eq!(handle.close().await, CloseOutcome::Closed);
        assert!(matches!(
            first.main().checkout(CheckoutClass::Batch).await,
            Err(CheckoutFailure::PoolClosed { .. })
        ));

        let second = handle.pools().expect("pools open again");
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(handle.close().await, CloseOutcome::Closed);
        assert_eq!(handle.close().await, CloseOutcome::AlreadyIdle);
    }

    #[tokio::test(start_paused = true)]
    async fn a_close_waits_for_a_write_in_flight_up_to_the_bound() {
        let handle = open_handle();
        let (_open, write) = handle.pools_for_write().expect("open pools");

        assert_eq!(
            handle.close().await,
            CloseOutcome::ClosedWithWritesUnfinished { unfinished: 1 }
        );
        drop(write);
    }

    /// Python `test_broker_postgres.py::TestCloseAsyncErrorPaths::test_close_async_attempts_both_on_first_failure`:
    /// the first step of a close that does not end well does not stop the
    /// second. A write that outlives the bound leaves its outcome unknown,
    /// and the pools close all the same.
    #[tokio::test(start_paused = true)]
    async fn a_close_whose_write_outlives_the_bound_still_closes_the_pools() {
        let handle = open_handle();
        let (open, write) = handle.pools_for_write().expect("open pools");

        assert_eq!(
            handle.close().await,
            CloseOutcome::ClosedWithWritesUnfinished { unfinished: 1 }
        );
        assert!(matches!(
            open.main().checkout(CheckoutClass::Batch).await,
            Err(CheckoutFailure::PoolClosed { .. })
        ));
        drop(write);
    }

    #[tokio::test]
    async fn a_close_after_the_writes_ended_does_not_wait() {
        let handle = open_handle();
        let (_open, write) = handle.pools_for_write().expect("open pools");
        drop(write);

        assert_eq!(handle.close().await, CloseOutcome::Closed);
    }
}

#[cfg(test)]
mod kept_session_tests {
    //! The session setting of the core pools on the test server: a connection that
    //! keeps its session runs `jit = off`, sent once in autocommit when the
    //! connection opens, so a rollback of a transaction on it keeps the
    //! setting. In PgBouncer transaction mode the main pool sets nothing (a
    //! session `SET` would reach other clients of the server connection), and
    //! the session pool still runs `jit = off`.
    //!
    //! Each read also returns `pg_settings.source`: a `SET` on the connection
    //! makes it `session`. So the tests hold on a server whose own setting is
    //! `jit = off`, where the value alone could not tell the two apart.

    use std::num::NonZeroU32;

    use super::*;
    use crate::db::{
        CheckoutClass, IsolationLevel, PooledConnection, RollbackOutcome, StatementBound,
        TypedStatement,
    };
    use crate::test_support::migrated_database_url;

    /// The value of `jit` on the connection and where it comes from.
    const JIT_SETTING: TypedStatement = TypedStatement::new(
        "test_jit_setting",
        "SELECT current_setting('jit') AS jit, \
                (SELECT source FROM pg_settings WHERE name = 'jit') AS source",
        &[],
    );

    /// A statement inside the transaction that the test rolls back.
    const STATEMENT_IN_TRANSACTION: TypedStatement =
        TypedStatement::new("test_statement_in_transaction", "SELECT 1", &[]);

    /// `jit` as the connection runs it: its value and its `pg_settings.source`.
    #[derive(Debug, PartialEq, Eq)]
    struct JitSetting {
        value: String,
        source: String,
    }

    impl JitSetting {
        fn set_off_on_the_session() -> JitSetting {
            JitSetting {
                value: "off".to_owned(),
                source: "session".to_owned(),
            }
        }
    }

    async fn jit_setting(connection: &mut PooledConnection) -> JitSetting {
        let rows = connection
            .query(
                &JIT_SETTING.bind().finish(),
                StatementBound::ConnectionTimeouts,
            )
            .await
            .expect("read the jit setting");
        let [row] = rows.as_slice() else {
            panic!("expected one row, got {}", rows.len());
        };
        JitSetting {
            value: row.try_get("jit").expect("the jit column"),
            source: row.try_get("source").expect("the source column"),
        }
    }

    /// A config of the test database whose pools hold one connection each.
    fn one_connection_config(database_url: &str) -> PostgresConfig {
        let mut config = PostgresConfig::from_url(database_url);
        config.pool_size = 1;
        config.max_overflow = 0;
        config
    }

    fn open_test_pools(config: &PostgresConfig) -> BrokerPools {
        BrokerPools::open_with_environment(
            config,
            PoolProfile::Producer,
            &ConnectionEnvironment::isolated(),
        )
        .expect("the pools of the test database build")
    }

    /// The setting survives a rollback of the first transaction.
    /// The connection opens at the first checkout, runs the setting, then the
    /// statement-pooling check; the transaction of the test rolls back after
    /// both, and the setting stays.
    #[tokio::test]
    async fn a_kept_session_runs_without_jit_after_a_transaction_rolls_back() {
        let url = migrated_database_url().await;
        let pools = open_test_pools(&one_connection_config(&url));
        let mut connection = pools
            .main()
            .checkout(CheckoutClass::Reserved)
            .await
            .expect("check out the only connection");

        let mut transaction = connection
            .begin(IsolationLevel::ServerDefault)
            .await
            .expect("begin");
        transaction
            .query(
                &STATEMENT_IN_TRANSACTION.bind().finish(),
                StatementBound::ConnectionTimeouts,
            )
            .await
            .expect("the statement in the transaction");
        let rollback = transaction.rollback().await;
        let setting = jit_setting(&mut connection).await;
        drop(connection);
        pools.close();

        assert!(
            matches!(rollback, RollbackOutcome::RolledBack),
            "{rollback:?}"
        );
        assert_eq!(setting, JitSetting::set_off_on_the_session());
    }

    /// The runtime pool in transaction mode keeps the server setting.
    /// The two URLs reach the same server directly and differ only in
    /// `application_name`, so the session URL gets a pool of its own.
    #[tokio::test]
    async fn in_transaction_mode_only_the_session_pool_turns_jit_off() {
        let url = migrated_database_url().await;
        let separator = if url.contains('?') { '&' } else { '?' };
        let mut config = one_connection_config(&url);
        config.session_database_url =
            Some(format!("{url}{separator}application_name=horsies_session"));
        config.pgbouncer_transaction_mode = true;
        let pools = open_test_pools(&config);
        let SessionPool::Separate { pool: session, .. } = &pools.session else {
            panic!("a distinct session URL gets a pool of its own");
        };

        let mut main_connection = pools
            .main()
            .checkout(CheckoutClass::Reserved)
            .await
            .expect("check out the main pool");
        let main_setting = jit_setting(&mut main_connection).await;
        let mut session_connection = session
            .checkout(CheckoutClass::Reserved)
            .await
            .expect("check out the session pool");
        let session_setting = jit_setting(&mut session_connection).await;
        drop(main_connection);
        drop(session_connection);
        pools.close();

        assert_ne!(main_setting.source, "session", "{main_setting:?}");
        assert_eq!(session_setting, JitSetting::set_off_on_the_session());
    }

    /// The most checks of the open count while a child pool opens its kept
    /// connections.
    const KEPT_OPEN_CHECKS: u32 = 100;
    const KEPT_OPEN_CHECK_EVERY: Duration = Duration::from_millis(50);

    /// Python `ConnectionPool(min_size=2).open()`: the connections open
    /// without a checkout, and the pool keeps no more than `min_size`.
    #[tokio::test]
    async fn a_child_pool_opens_its_kept_connections_without_a_checkout() {
        let url = migrated_database_url().await;
        let mut config = PostgresConfig::from_url(&url);
        config.pool_size = 3;
        config.max_overflow = 0;
        let pools = BrokerPools::open_with_environment(
            &config,
            PoolProfile::Child {
                kept_open: KeptOpen::Connections(NonZeroU32::new(2).expect("two")),
                check: true,
            },
            &ConnectionEnvironment::isolated(),
        )
        .expect("the pools of the test database build");
        let mut status = pools.main().status();
        for _ in 0..KEPT_OPEN_CHECKS {
            if status.open >= 2 && status.idle >= 2 {
                break;
            }
            tokio::time::sleep(KEPT_OPEN_CHECK_EVERY).await;
            status = pools.main().status();
        }
        pools.close();

        assert_eq!(status.open, 2, "{status:?}");
        assert_eq!(status.idle, 2, "{status:?}");
    }
}

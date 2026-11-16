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
//!   checkout wait, `pool_recycle` as the connection age limit, and the 1 s
//!   idle ping when `pool_pre_ping` is on. The main pool sends unnamed
//!   statements only in PgBouncer transaction mode; the session pool always
//!   keeps its session.
//!
//! Building the pools connects to nothing: a connection opens at the first
//! checkout.

use std::sync::Arc;
use std::time::Duration;

use crate::core::PostgresConfig;
use crate::db::{
    parse_connection_url, ConnectPlan, ConnectPlanError, ConnectionAge, ConnectionEnvironment,
    CorePool, EnvironmentError, IdlePing, PoolPlan, PoolRole, PoolSizing, PoolSizingError, Reserve,
    StatementProtocol, UrlError, DEFAULT_IDLE_PING_AFTER, DEFAULT_SOCKET_SETTINGS,
    WORKER_RESERVED_CONNECTIONS,
};

/// The process a broker's pools serve.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoolProfile {
    /// A producer or a monitoring handle: the main URL pool keeps no reserve.
    Producer,
    /// A worker parent: the main URL pool is the worker profile pool, which
    /// keeps [`WORKER_RESERVED_CONNECTIONS`] free from batch checkouts.
    Worker,
}

/// The pool that session work (schema work, partition maintenance) uses.
pub(crate) enum SessionPool {
    /// `session_database_url` is absent or equal to `database_url`: session
    /// work uses the main pool.
    SameAsMain,
    /// A pool of its own on `session_database_url`.
    Separate(CorePool),
}

/// The core pools of one broker.
pub(crate) struct BrokerPools {
    main: CorePool,
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
    /// Builds the pools of `config` for `profile`.
    ///
    /// Reads the process environment (`PGPASSWORD`, `HOME`, the set libpq
    /// variables), then, per URL, the TLS files the URL names, `~/.pgpass`
    /// when no password is given, and the process user name when the URL
    /// names no user. Connects to nothing.
    ///
    /// Errors: [`PoolSettingsError`]; nothing was built.
    pub(crate) fn open(
        config: &PostgresConfig,
        profile: PoolProfile,
    ) -> Result<BrokerPools, PoolSettingsError> {
        let environment =
            ConnectionEnvironment::capture().map_err(PoolSettingsError::Environment)?;
        BrokerPools::open_with_environment(config, profile, &environment)
    }

    /// Builds the pools of `config` with an environment the caller read.
    /// Reads the files [`BrokerPools::open`] names; connects to nothing.
    pub(crate) fn open_with_environment(
        config: &PostgresConfig,
        profile: PoolProfile,
        environment: &ConnectionEnvironment,
    ) -> Result<BrokerPools, PoolSettingsError> {
        let main_shape = main_pool_shape(profile);
        let main = open_pool(
            &config.database_url,
            pool_plan(
                config,
                main_shape.role,
                main_shape.reserve,
                runtime_protocol(config),
            )
            .map_err(|cause| PoolSettingsError::Sizing {
                role: main_shape.role,
                cause,
            })?,
            environment,
        )?;
        let session_url = config.effective_session_database_url();
        let session = match session_url == config.database_url {
            true => SessionPool::SameAsMain,
            false => SessionPool::Separate(open_pool(
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
            )?),
        };
        Ok(BrokerPools { main, session })
    }

    /// The pool on `database_url`.
    pub(crate) fn main(&self) -> &CorePool {
        &self.main
    }

    /// Closes every pool: later checkouts fail with `PoolClosed`. Checked-out
    /// connections end when their guards drop.
    pub(crate) fn close(&self) {
        self.main.close();
        match &self.session {
            SessionPool::SameAsMain => {}
            SessionPool::Separate(pool) => pool.close(),
        }
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
        max_age: ConnectionAge::ReplaceAfter(Duration::from_secs(u64::from(config.pool_recycle))),
        protocol,
    })
}

/// Parses one URL, loads its connection plan and builds the pool.
fn open_pool(
    url: &str,
    plan: PoolPlan,
    environment: &ConnectionEnvironment,
) -> Result<CorePool, PoolSettingsError> {
    let role = plan.role;
    let settings = parse_connection_url(url, environment)
        .map_err(|cause| PoolSettingsError::Url { role, cause })?;
    let connect = ConnectPlan::load(&settings, environment, &DEFAULT_SOCKET_SETTINGS)
        .map_err(|cause| PoolSettingsError::Plan { role, cause })?;
    Ok(CorePool::new(plan, Arc::new(connect)))
}

/// The URL of a test's sqlx connect options, in the form the statement
/// layer reads: sqlx's own rendering (`ConnectOptions::to_url_lossy`)
/// without its `statement-cache-capacity` parameter, which only sqlx reads.
/// Test options set no option that the rendering drops (`application_name`,
/// `options`).
#[cfg(test)]
pub(crate) fn test_url_of(options: &sqlx::postgres::PgConnectOptions) -> String {
    use sqlx::ConnectOptions;

    let mut url = options.to_url_lossy();
    let kept: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(name, _)| *name != "statement-cache-capacity")
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect();
    url.set_query(None);
    if !kept.is_empty() {
        url.query_pairs_mut().extend_pairs(kept);
    }
    url.to_string()
}

/// Core pools on the database of a test's sqlx pool, for brokers that tests
/// build from such a pool. The environment is isolated: test URLs carry
/// their password.
#[cfg(test)]
pub(crate) fn pools_for_test_pool(pool: &sqlx::PgPool) -> BrokerPools {
    BrokerPools::open_with_environment(
        &PostgresConfig::from_url(test_url_of(&pool.connect_options())),
        PoolProfile::Producer,
        &ConnectionEnvironment::isolated(),
    )
    .expect("the URL of a test pool is accepted by the statement layer")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> PostgresConfig {
        let mut config =
            PostgresConfig::from_url("postgresql://horsies:secret@db.example:5432/app");
        config.pool_size = 5;
        config.max_overflow = 3;
        config.pool_timeout = 7;
        config.pool_recycle = 1800;
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
            &pools.session,
            SessionPool::Separate(pool) if pool.role() == PoolRole::Session
        ));
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
}

#[cfg(test)]
mod kept_session_tests {
    //! The session setting of the core pools on the test server (Python
    //! `tests/integration/test_broker_session_settings.py`): a connection that
    //! keeps its session runs `jit = off`, sent once in autocommit when the
    //! connection opens, so a rollback of a transaction on it keeps the
    //! setting. In PgBouncer transaction mode the main pool sets nothing (a
    //! session `SET` would reach other clients of the server connection), and
    //! the session pool still runs `jit = off`.
    //!
    //! Each read also returns `pg_settings.source`: a `SET` on the connection
    //! makes it `session`. So the tests hold on a server whose own setting is
    //! `jit = off`, where the value alone could not tell the two apart.

    use serial_test::serial;

    use super::*;
    use crate::broker::terminalization_matrix::migrated_database_url;
    use crate::db::{
        CheckoutClass, IsolationLevel, PooledConnection, RollbackOutcome, StatementBound,
        TypedStatement,
    };

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

    /// Python `test_broker_session_settings.py::test_setting_survives_a_rollback_of_the_first_transaction`.
    /// The connection opens at the first checkout, runs the setting, then the
    /// statement-pooling check; the transaction of the test rolls back after
    /// both, and the setting stays.
    #[tokio::test]
    #[serial]
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

    /// Python `test_broker_session_settings.py::test_transaction_mode_runtime_pool_keeps_the_server_setting`.
    /// The two URLs reach the same server directly and differ only in
    /// `application_name`, so the session URL gets a pool of its own.
    #[tokio::test]
    #[serial]
    async fn in_transaction_mode_only_the_session_pool_turns_jit_off() {
        let url = migrated_database_url().await;
        let separator = if url.contains('?') { '&' } else { '?' };
        let mut config = one_connection_config(&url);
        config.session_database_url =
            Some(format!("{url}{separator}application_name=horsies_session"));
        config.pgbouncer_transaction_mode = true;
        let pools = open_test_pools(&config);
        let SessionPool::Separate(session) = &pools.session else {
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
}

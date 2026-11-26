use std::num::NonZeroU32;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};

use crate::core::error::{ErrorCode, HorsiesError, ValidationReport};

use crate::core::config::no_member_after_the_tag;
use crate::core::config::socket::input::{
    SocketValueRefused, TcpKeepalivesConfig, TcpUserTimeoutConfig,
};
use crate::core::config::socket::socket_settings;
use crate::db::SocketSettings;

/// The persistent connections of the general broker pool of a config that
/// names no `pool_size`. It is the one source of the default of both
/// clients: `horsies-codegen` writes it into the Python client
/// (`horsies._catalog.broker_pool_defaults`).
pub const BROKER_POOL_SIZE_DEFAULT: u32 = 5;

/// The connections past the pool size that the general broker pool of a
/// config that names no `max_overflow` opens under load. It is the one
/// source of the default of both clients, as [`BROKER_POOL_SIZE_DEFAULT`].
pub const BROKER_MAX_OVERFLOW_DEFAULT: u32 = 10;

/// PostgreSQL broker configuration.
///
/// `Debug` is implemented manually to redact the password in the connection
/// URLs so structured logs / panics never leak credentials. Parity with
/// horsies PR #101 f368965c.
///
/// Deserialization refuses an unknown key, and refuses each pool field that
/// 0.7.0 removed by name ([`RemovedPoolField`]).
#[derive(Clone, Serialize)]
pub struct PostgresConfig {
    /// PostgreSQL runtime connection URL.
    ///
    /// This may point at a PgBouncer transaction-pool endpoint when
    /// `pgbouncer_transaction_mode` is true.
    pub database_url: String,

    /// Where the session pool connects: schema initialization, partition
    /// maintenance, and LISTEN/NOTIFY. Default:
    /// [`SessionDatabase::SameAsDatabaseUrl`].
    ///
    /// PgBouncer transaction pooling cannot preserve session state for
    /// LISTEN/NOTIFY, so pooled runtime deployments must name a direct or
    /// session-pooled URL here ([`SessionDatabase::Separate`]).
    #[serde(default)]
    pub session_database: SessionDatabase,

    /// Configure the runtime pool for PgBouncer transaction pooling.
    ///
    /// SQLx uses protocol-level prepared statements for parameterized
    /// Postgres queries. The PgBouncer endpoint must therefore have prepared
    /// statement tracking enabled (`max_prepared_statements > 0`). This flag
    /// disables SQLx's local statement cache and requires a separate
    /// `session_database` so schema initialization and LISTEN/NOTIFY continue
    /// to use a session-capable connection.
    #[serde(default)]
    pub pgbouncer_transaction_mode: bool,

    /// Whether to pre-ping the connection pool.
    #[serde(default = "default_true")]
    pub pool_pre_ping: bool,

    /// Whether the pool of the worker process pings a connection before use,
    /// by the rule of `pool_pre_ping`. Off by default: the worker process
    /// sends most of its writes as one statement in autocommit, and a ping
    /// adds a round trip to the checkout of a connection that waited in the
    /// pool for more than 1 s. With it off, a dead pooled connection fails
    /// the first statement on it with a retryable connection error.
    /// `pool_pre_ping` applies to every other pool, also to the pool that
    /// task code in a worker child sends through.
    #[serde(default)]
    pub worker_pool_pre_ping: bool,

    /// The persistent connections of the general broker pool: the pool of
    /// a producer, a web process, a scheduler or an app API. Default
    /// [`BROKER_POOL_SIZE_DEFAULT`]. It does not size the pools of a
    /// worker: the worker process opens `WorkerProfile.pool`, and task code
    /// in a worker child opens the `task_send_pool` of its child kind.
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,

    /// The connections past `pool_size` that the general broker pool opens
    /// under load. Default [`BROKER_MAX_OVERFLOW_DEFAULT`].
    #[serde(default = "default_max_overflow")]
    pub max_overflow: u32,

    /// Default policy for retaining canonical rerun input at enqueue.
    #[serde(default)]
    pub retain_rerun_input_default: bool,

    /// Timeout in seconds for acquiring a connection from the pool.
    #[serde(default = "default_pool_timeout_seconds")]
    pub pool_timeout_seconds: u32,

    /// When a connection is replaced by its age (SQLAlchemy's
    /// `pool_recycle`): never, or after a number of seconds.
    #[serde(default = "default_pool_recycle")]
    pub pool_recycle: crate::manifest::PoolRecycle,

    /// Whether to log SQL statements.
    #[serde(default)]
    pub echo: bool,

    /// TCP keepalives of every core connection (Python `tcp_keepalives`).
    /// Default: on, 30 s, 10 s, 3 probes. [`Self::validate`] refuses a value
    /// outside its bound.
    #[serde(default)]
    pub tcp_keepalives: TcpKeepalivesConfig,

    /// TCP user timeout of every core connection (Python
    /// `tcp_user_timeout`). Default: 30,000 ms. [`Self::validate`] refuses a
    /// value outside its bound.
    #[serde(default)]
    pub tcp_user_timeout: TcpUserTimeoutConfig,
}

/// Where the session pool connects (Python
/// `SessionDatabaseSameAsDatabaseUrl`, `SessionDatabaseSeparate`). The JSON
/// form is `{"kind": "SAME_AS_DATABASE_URL"}` or
/// `{"kind": "SEPARATE", "url": "..."}`. `Debug` masks the password of the
/// URL.
#[derive(Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum SessionDatabase {
    /// The session pool uses `database_url`.
    #[default]
    #[serde(deserialize_with = "no_member_after_the_tag")]
    SameAsDatabaseUrl,
    /// The session pool uses its own direct or session-capable URL.
    Separate { url: String },
}

impl std::fmt::Debug for SessionDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SameAsDatabaseUrl => f.write_str("SameAsDatabaseUrl"),
            Self::Separate { url } => f
                .debug_struct("Separate")
                .field("url", &mask_url(url))
                .finish(),
        }
    }
}

/// Redact the password in a `scheme://user:PASS@host/db` URL, leaving the rest
/// intact for diagnostics. URLs without embedded credentials are returned as-is.
fn mask_url(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('@') {
            Some((userinfo, host)) => match userinfo.split_once(':') {
                Some((user, _pass)) => format!("{}://{}:****@{}", scheme, user, host),
                None => url.to_owned(),
            },
            None => url.to_owned(),
        },
        None => url.to_owned(),
    }
}

impl std::fmt::Debug for PostgresConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresConfig")
            .field("database_url", &mask_url(&self.database_url))
            .field("session_database", &self.session_database)
            .field(
                "pgbouncer_transaction_mode",
                &self.pgbouncer_transaction_mode,
            )
            .field("pool_pre_ping", &self.pool_pre_ping)
            .field("worker_pool_pre_ping", &self.worker_pool_pre_ping)
            .field("pool_size", &self.pool_size)
            .field("max_overflow", &self.max_overflow)
            .field(
                "retain_rerun_input_default",
                &self.retain_rerun_input_default,
            )
            .field("pool_timeout_seconds", &self.pool_timeout_seconds)
            .field("pool_recycle", &self.pool_recycle)
            .field("echo", &self.echo)
            .field("tcp_keepalives", &self.tcp_keepalives)
            .field("tcp_user_timeout", &self.tcp_user_timeout)
            .finish()
    }
}

fn default_true() -> bool {
    true
}
fn default_pool_size() -> u32 {
    BROKER_POOL_SIZE_DEFAULT
}
fn default_max_overflow() -> u32 {
    BROKER_MAX_OVERFLOW_DEFAULT
}
fn default_pool_timeout_seconds() -> u32 {
    30
}
fn default_pool_recycle() -> crate::manifest::PoolRecycle {
    crate::manifest::PoolRecycle::after_seconds(1800)
}

/// Every field of `PostgresConfig`. An unknown key is refused, as Python's
/// config models refuse extra keys.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PostgresConfigWire {
    database_url: String,
    #[serde(default)]
    session_database: SessionDatabase,
    #[serde(default)]
    pgbouncer_transaction_mode: bool,
    #[serde(default = "default_true")]
    pool_pre_ping: bool,
    #[serde(default)]
    worker_pool_pre_ping: bool,
    #[serde(default = "default_pool_size")]
    pool_size: u32,
    #[serde(default = "default_max_overflow")]
    max_overflow: u32,
    #[serde(default)]
    retain_rerun_input_default: bool,
    #[serde(default = "default_pool_timeout_seconds")]
    pool_timeout_seconds: u32,
    #[serde(default = "default_pool_recycle")]
    pool_recycle: crate::manifest::PoolRecycle,
    #[serde(default)]
    echo: bool,
    #[serde(default)]
    tcp_keepalives: TcpKeepalivesConfig,
    #[serde(default)]
    tcp_user_timeout: TcpUserTimeoutConfig,
}

impl<'de> Deserialize<'de> for PostgresConfig {
    /// Refuses every removed pool field that the config names, in one error
    /// ([`removed_pool_field_refusal`]), before the other keys are read.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if let Some(fields) = value.as_object() {
            removed_pool_field_refusal(fields).map_err(D::Error::custom)?;
        }
        let wire: PostgresConfigWire = serde_json::from_value(value).map_err(D::Error::custom)?;
        Ok(Self {
            database_url: wire.database_url,
            session_database: wire.session_database,
            pgbouncer_transaction_mode: wire.pgbouncer_transaction_mode,
            pool_pre_ping: wire.pool_pre_ping,
            worker_pool_pre_ping: wire.worker_pool_pre_ping,
            pool_size: wire.pool_size,
            max_overflow: wire.max_overflow,
            retain_rerun_input_default: wire.retain_rerun_input_default,
            pool_timeout_seconds: wire.pool_timeout_seconds,
            pool_recycle: wire.pool_recycle,
            echo: wire.echo,
            tcp_keepalives: wire.tcp_keepalives,
            tcp_user_timeout: wire.tcp_user_timeout,
        })
    }
}

/// A pool field that 0.7.0 removed from the broker config. Each child kind
/// of a worker profile has its own pool, and the worker process has its own
/// (`WorkerProfile.pool`). A config that names a removed field is refused
/// with the field that replaces it, never ignored. The refusal reads no
/// stored data; it refuses input only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovedPoolField {
    /// Replaced by `min_size` of the pool of each child kind.
    WorkerChildPoolMinSize,
    /// Replaced by `max_size` of the pool of each child kind.
    WorkerChildPoolMaxSize,
    /// Replaced by `check` of the pool of each child kind.
    WorkerChildPoolCheck,
    /// Replaced by the pool of the worker process in the worker profile.
    WorkerPoolSize,
    /// The pool of the worker process has no overflow.
    WorkerMaxOverflow,
}

impl RemovedPoolField {
    /// Every removed field, in the order that a refusal reports them: the
    /// child pool fields, then the worker pool fields (Python
    /// `MOVED_TO_WORKER_PROFILE`, then `MOVED_TO_PARENT_POOL`).
    pub const ALL: [Self; 5] = [
        Self::WorkerChildPoolMinSize,
        Self::WorkerChildPoolMaxSize,
        Self::WorkerChildPoolCheck,
        Self::WorkerPoolSize,
        Self::WorkerMaxOverflow,
    ];

    /// The config key of the field.
    pub const fn key(self) -> &'static str {
        match self {
            Self::WorkerChildPoolMinSize => "worker_child_pool_min_size",
            Self::WorkerChildPoolMaxSize => "worker_child_pool_max_size",
            Self::WorkerChildPoolCheck => "worker_child_pool_check",
            Self::WorkerPoolSize => "worker_pool_size",
            Self::WorkerMaxOverflow => "worker_max_overflow",
        }
    }

    /// The refusal of the field, with `CONFIG_INVALID_BROKER_POOL` and the
    /// text of Python's refusal.
    pub fn refusal(self) -> HorsiesError {
        let refusal = HorsiesError::new(format!("{} was removed in 0.7.0", self.key()))
            .with_code(ErrorCode::ConfigInvalidBrokerPool);
        match self {
            Self::WorkerChildPoolMinSize => child_pool_refusal(refusal, "min_size=N"),
            Self::WorkerChildPoolMaxSize => {
                child_pool_refusal(refusal, "max_size=PoolSize(connections=N)")
            }
            Self::WorkerChildPoolCheck => child_pool_refusal(refusal, "pre_ping=False"),
            Self::WorkerPoolSize => parent_pool_refusal(
                refusal,
                "the worker process opens its pool with the resolved pool.max_size \
                 of its worker profile",
            ),
            Self::WorkerMaxOverflow => parent_pool_refusal(
                refusal,
                "the pool of the worker process has no overflow: it holds at most \
                 pool.max_size connections",
            ),
        }
    }
}

/// The note and help of a removed child pool field: the pool argument of
/// each child kind that replaces it.
fn child_pool_refusal(refusal: HorsiesError, pool_argument: &str) -> HorsiesError {
    refusal
        .with_note("each child kind of a worker profile has its own connection pool")
        .with_help(format!(
            "set the pool of each child kind in AppConfig.worker_profiles: \
             IsolatedChildren(pool=IsolatedPool({pool_argument})), \
             SharedAsyncChildren(pool=SharedPool({pool_argument})) or \
             SharedThreadChildren(pool=SharedPool({pool_argument}))"
        ))
}

/// The notes and help of a removed worker pool field: the pool of the
/// worker process in the worker profile replaces it.
fn parent_pool_refusal(refusal: HorsiesError, note: &str) -> HorsiesError {
    refusal
        .with_note(note)
        .with_note(
            "task code in a worker child that sends opens the task_send_pool of its \
             child kind, not this pool",
        )
        .with_help(
            "set the pool of the worker process in AppConfig.worker_profiles: \
             WorkerProfile(pool=ParentPool(max_size=PoolSize(connections=N))); the \
             default, PoolSizeFromSeats(), derives it from the seats of the profile",
        )
}

/// Refuse the removed pool fields that `fields` names, every one in one
/// error, in the order of [`RemovedPoolField::ALL`]. One field gives its own
/// refusal; two or more give one error that carries each refusal as a note.
/// Any value is refused, `null` included. Pure.
pub fn removed_pool_field_refusal(
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), HorsiesError> {
    let mut report = ValidationReport::new("database");
    for field in RemovedPoolField::ALL {
        if fields.contains_key(field.key()) {
            report.add(field.refusal());
        }
    }
    report.into_result()
}

/// Validation error for PostgresConfig.
///
/// Every variant is a refusal of the settings themselves: the same settings
/// are refused again, so none is retryable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PostgresConfigError {
    /// `database_url` does not start with `postgresql://` or `postgres://`.
    /// The value is the scheme only (the text before `://`), or
    /// [`NO_URL_SCHEME`] when the URL has none, so no credential is echoed.
    #[error(
        "invalid database_url scheme {0:?}: the URL must start with 'postgresql://' or \
         'postgres://'; a driver suffix such as 'postgresql+psycopg://' is refused, \
         write 'postgresql://' instead"
    )]
    InvalidUrlScheme(String),
    /// The URL of a separate `session_database` does not start with
    /// `postgresql://` or `postgres://`. The value is the scheme only, as in
    /// [`PostgresConfigError::InvalidUrlScheme`].
    #[error(
        "invalid session_database.url scheme {0:?}: the URL must start with 'postgresql://' \
         or 'postgres://'; a driver suffix such as 'postgresql+psycopg://' is refused, \
         write 'postgresql://' instead"
    )]
    InvalidSessionUrlScheme(String),
    #[error("pgbouncer_transaction_mode=true requires a separate session_database for schema initialization, partition maintenance, and LISTEN/NOTIFY")]
    MissingSessionDatabase,
    #[error(
        "session_database must not use the transaction-pool URL: partition maintenance \
         and LISTEN require PostgreSQL session affinity; use a direct or session-capable \
         URL for session_database"
    )]
    SessionUrlIsTransactionPoolUrl,
    /// A TCP keepalive or user timeout value is outside its bound
    /// (Python `CONFIG_INVALID_KEEPALIVE`). The cause names the field and
    /// the bound, never the value.
    #[error("{0}")]
    SocketValueOutOfBounds(SocketValueRefused),
}

/// The scheme reported for a URL that has no `://`.
pub const NO_URL_SCHEME: &str = "<no scheme>";

/// The scheme of a URL (the text before `://`), or [`NO_URL_SCHEME`].
/// Never returns text after `://`, where credentials live.
fn url_scheme(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, _rest)) => scheme.to_owned(),
        None => NO_URL_SCHEME.to_owned(),
    }
}

impl PostgresConfig {
    /// Create a PostgresConfig from just a database URL, using defaults for
    /// all other fields.
    ///
    /// This mirrors the Python convenience of `PostgresConfig(database_url="...")`.
    pub fn from_database_url(url: impl Into<String>) -> Self {
        Self {
            database_url: url.into(),
            session_database: SessionDatabase::SameAsDatabaseUrl,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: default_true(),
            worker_pool_pre_ping: false,
            pool_size: default_pool_size(),
            max_overflow: default_max_overflow(),
            retain_rerun_input_default: false,
            pool_timeout_seconds: default_pool_timeout_seconds(),
            pool_recycle: default_pool_recycle(),
            echo: false,
            tcp_keepalives: TcpKeepalivesConfig::default(),
            tcp_user_timeout: TcpUserTimeoutConfig::default(),
        }
    }

    /// The socket settings of every core connection of this config.
    pub fn socket_settings(&self) -> SocketSettings {
        socket_settings(self.tcp_keepalives, self.tcp_user_timeout)
    }

    /// Create a PostgresConfig for PgBouncer transaction pooling.
    ///
    /// `database_url` should be the transaction-pool endpoint. `session_database_url`
    /// becomes the separate session database: a direct or session-pooled
    /// endpoint used for schema work, partition maintenance, and LISTEN/NOTIFY. The transaction-pool endpoint must support
    /// protocol-level prepared statement tracking (`max_prepared_statements > 0`).
    pub fn from_pgbouncer_urls(
        database_url: impl Into<String>,
        session_database_url: impl Into<String>,
    ) -> Self {
        let mut config = Self::from_database_url(database_url);
        config.session_database = SessionDatabase::Separate {
            url: session_database_url.into(),
        };
        config.pgbouncer_transaction_mode = true;
        config
    }

    /// Return the URL used for schema initialization, partition maintenance,
    /// and LISTEN/NOTIFY.
    pub fn effective_session_database_url(&self) -> &str {
        match &self.session_database {
            SessionDatabase::SameAsDatabaseUrl => &self.database_url,
            SessionDatabase::Separate { url } => url,
        }
    }

    /// A copy whose pool holds up to `connections` connections with no
    /// overflow: `pool_size = connections`, `max_overflow = 0`. Every other
    /// field is kept, `pool_pre_ping` included. Task code in a worker child
    /// sends through a pool of this form (the `task_send_pool` of its kind).
    pub fn with_exact_pool(&self, connections: NonZeroU32) -> Self {
        Self {
            pool_size: connections.get(),
            max_overflow: 0,
            ..self.clone()
        }
    }

    /// The broker config of the worker process: a pool of up to
    /// `parent_pool` connections with no overflow ([`Self::with_exact_pool`]),
    /// the resolved pool of its worker profile, with `worker_pool_pre_ping`
    /// as its `pool_pre_ping`.
    pub fn worker_process_config(&self, parent_pool: NonZeroU32) -> Self {
        Self {
            pool_pre_ping: self.worker_pool_pre_ping,
            ..self.with_exact_pool(parent_pool)
        }
    }

    /// Validate the configuration.
    ///
    /// Accepts only the `postgresql://` and `postgres://` schemes for both
    /// URLs. In PgBouncer transaction mode, requires a separate session
    /// database whose URL differs from `database_url`. Then refuses the
    /// first TCP keepalive or user timeout value outside its bound
    /// ([`PostgresConfigError::SocketValueOutOfBounds`]).
    pub fn validate(&self) -> Result<(), PostgresConfigError> {
        if !is_postgres_url(&self.database_url) {
            return Err(PostgresConfigError::InvalidUrlScheme(url_scheme(
                &self.database_url,
            )));
        }
        match (&self.session_database, self.pgbouncer_transaction_mode) {
            (SessionDatabase::Separate { url }, _) if !is_postgres_url(url) => {
                return Err(PostgresConfigError::InvalidSessionUrlScheme(url_scheme(
                    url,
                )));
            }
            (SessionDatabase::SameAsDatabaseUrl, true) => {
                return Err(PostgresConfigError::MissingSessionDatabase);
            }
            (SessionDatabase::Separate { url }, true) if *url == self.database_url => {
                return Err(PostgresConfigError::SessionUrlIsTransactionPoolUrl);
            }
            (SessionDatabase::SameAsDatabaseUrl, false) | (SessionDatabase::Separate { .. }, _) => {
            }
        }
        self.tcp_keepalives
            .checked()
            .map_err(PostgresConfigError::SocketValueOutOfBounds)?;
        self.tcp_user_timeout
            .checked()
            .map_err(PostgresConfigError::SocketValueOutOfBounds)?;
        Ok(())
    }
}

fn is_postgres_url(url: &str) -> bool {
    url.starts_with("postgresql://") || url.starts_with("postgres://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_url_redacts_password() {
        assert_eq!(
            mask_url("postgresql://user:secret@localhost:5432/db"),
            "postgresql://user:****@localhost:5432/db"
        );
    }

    #[test]
    fn mask_url_leaves_credential_free_urls() {
        assert_eq!(
            mask_url("postgresql://localhost:5432/db"),
            "postgresql://localhost:5432/db"
        );
        assert_eq!(mask_url("not a url"), "not a url");
    }

    #[test]
    fn debug_does_not_leak_password() {
        let config = PostgresConfig {
            database_url: "postgresql://user:topsecret@localhost/db".to_owned(),
            session_database: SessionDatabase::Separate {
                url: "postgresql://user:topsecret@localhost/db".to_owned(),
            },
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            worker_pool_pre_ping: false,
            pool_size: 30,
            max_overflow: 10,
            retain_rerun_input_default: false,
            pool_timeout_seconds: 30,
            pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
            echo: false,
            tcp_keepalives: Default::default(),
            tcp_user_timeout: Default::default(),
        };
        let rendered = format!("{:?}", config);
        assert!(!rendered.contains("topsecret"), "Debug leaked the password");
        assert!(rendered.contains("user:****@localhost"));
    }

    #[test]
    fn valid_url() {
        let config = PostgresConfig {
            database_url: "postgresql://user:pass@localhost/db".to_owned(),
            session_database: SessionDatabase::SameAsDatabaseUrl,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            worker_pool_pre_ping: false,
            pool_size: 30,
            max_overflow: 30,
            retain_rerun_input_default: false,
            pool_timeout_seconds: 30,
            pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
            echo: false,
            tcp_keepalives: Default::default(),
            tcp_user_timeout: Default::default(),
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn postgres_scheme_also_valid() {
        let config = PostgresConfig {
            database_url: "postgres://user:pass@localhost/db".to_owned(),
            session_database: SessionDatabase::SameAsDatabaseUrl,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            worker_pool_pre_ping: false,
            pool_size: 30,
            max_overflow: 30,
            retain_rerun_input_default: false,
            pool_timeout_seconds: 30,
            pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
            echo: false,
            tcp_keepalives: Default::default(),
            tcp_user_timeout: Default::default(),
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn invalid_url_scheme() {
        let config = PostgresConfig {
            database_url: "mysql://user:pass@localhost/db".to_owned(),
            session_database: SessionDatabase::SameAsDatabaseUrl,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            worker_pool_pre_ping: false,
            pool_size: 30,
            max_overflow: 30,
            retain_rerun_input_default: false,
            pool_timeout_seconds: 30,
            pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
            echo: false,
            tcp_keepalives: Default::default(),
            tcp_user_timeout: Default::default(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn defaults_from_json() {
        let json = r#"{"database_url": "postgresql://localhost/db"}"#;
        let config: PostgresConfig = serde_json::from_str(json).unwrap();
        assert!(config.pool_pre_ping);
        assert!(!config.echo);
        assert_eq!(config.session_database, SessionDatabase::SameAsDatabaseUrl);
        assert!(!config.pgbouncer_transaction_mode);
        assert!(!config.retain_rerun_input_default);
    }

    /// A config that names no pool gets the general broker pool of 5
    /// connections and 10 more under load, by serde and by `from_database_url`.
    #[test]
    fn the_general_broker_pool_default_is_5_and_10() {
        let parsed: PostgresConfig =
            serde_json::from_str(r#"{"database_url": "postgresql://localhost/db"}"#)
                .expect("a config with only a URL is valid");
        let built = PostgresConfig::from_database_url("postgresql://localhost/db");

        assert_eq!((parsed.pool_size, parsed.max_overflow), (5, 10));
        assert_eq!((built.pool_size, built.max_overflow), (5, 10));
    }

    #[test]
    fn pgbouncer_constructor_sets_split_urls() {
        let config = PostgresConfig::from_pgbouncer_urls(
            "postgresql://user:pass@pooler/db",
            "postgresql://user:pass@direct/db",
        );

        assert_eq!(config.database_url, "postgresql://user:pass@pooler/db");
        assert_eq!(
            config.effective_session_database_url(),
            "postgresql://user:pass@direct/db",
        );
        assert!(config.pgbouncer_transaction_mode);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn pgbouncer_requires_session_database_url() {
        let mut config = PostgresConfig::from_database_url("postgresql://user:pass@pooler/db");
        config.pgbouncer_transaction_mode = true;

        assert!(matches!(
            config.validate(),
            Err(PostgresConfigError::MissingSessionDatabase),
        ));
    }

    /// A TCP keepalive value outside its bound is refused by `validate`,
    /// with the field path and the bound.
    #[test]
    fn a_socket_value_outside_its_bound_is_refused() {
        let mut config = PostgresConfig::from_database_url("postgresql://user:pass@localhost/db");
        config.tcp_keepalives = TcpKeepalivesConfig::On {
            idle_seconds: 30,
            interval_seconds: 10,
            probes: 0,
        };
        let Err(PostgresConfigError::SocketValueOutOfBounds(refused)) = config.validate() else {
            panic!("zero probes are refused");
        };
        assert_eq!(refused.field, "tcp_keepalives.probes");

        config.tcp_keepalives = TcpKeepalivesConfig::Off;
        config.tcp_user_timeout = TcpUserTimeoutConfig::After { milliseconds: 0 };
        let Err(PostgresConfigError::SocketValueOutOfBounds(refused)) = config.validate() else {
            panic!("zero milliseconds are refused");
        };
        assert_eq!(refused.field, "tcp_user_timeout.milliseconds");
    }

    /// The JSON form of a separate session database names its URL, and the
    /// `Debug` form masks its password.
    #[test]
    fn a_separate_session_database_parses_and_masks_its_password() {
        let config: PostgresConfig = serde_json::from_str(
            r#"{"database_url": "postgresql://localhost/db",
                "session_database": {"kind": "SEPARATE", "url": "postgresql://u:topsecret@direct/db"}}"#,
        )
        .expect("a separate session database parses");
        assert_eq!(
            config.effective_session_database_url(),
            "postgresql://u:topsecret@direct/db"
        );
        assert!(!format!("{:?}", config.session_database).contains("topsecret"));
        assert!(serde_json::from_str::<PostgresConfig>(
            r#"{"database_url": "postgresql://localhost/db",
                "session_database_url": "postgresql://localhost/db"}"#,
        )
        .is_err());
    }

    /// Python `PostgresConfig.validate_pgbouncer_configuration`: in PgBouncer
    /// mode the session URL must differ from the transaction-pool URL.
    #[test]
    fn pgbouncer_session_url_must_differ_from_the_pool_url() {
        let config = PostgresConfig::from_pgbouncer_urls(
            "postgresql://user:pass@pooler/db",
            "postgresql://user:pass@pooler/db",
        );
        assert!(matches!(
            config.validate(),
            Err(PostgresConfigError::SessionUrlIsTransactionPoolUrl),
        ));

        // Outside PgBouncer mode the two roles may share one URL.
        let mut direct = PostgresConfig::from_database_url("postgresql://user:pass@direct/db");
        direct.session_database = SessionDatabase::Separate {
            url: "postgresql://user:pass@direct/db".to_owned(),
        };
        assert!(direct.validate().is_ok());
    }

    #[test]
    fn unknown_key_is_refused() {
        let error = serde_json::from_str::<PostgresConfig>(
            r#"{"database_url": "postgresql://localhost/db", "pool_sizes": 3}"#,
        )
        .expect_err("an unknown key must be refused");
        assert!(error.to_string().contains("unknown field"), "{error}");
    }

    fn connections(count: u32) -> NonZeroU32 {
        NonZeroU32::new(count).expect("a test pool holds at least one connection")
    }

    /// The first line of each refusal that a collected refusal carries.
    fn first_lines(refusal: &HorsiesError) -> Vec<&str> {
        refusal
            .notes
            .iter()
            .map(|note| note.lines().next().unwrap_or_default())
            .collect()
    }

    /// Python `test_app_config.py::TestPostgresConfigPgBouncer::test_the_worker_process_does_not_pre_ping_by_default`.
    #[test]
    fn the_worker_process_does_not_pre_ping_by_default() {
        let config: PostgresConfig =
            serde_json::from_str(r#"{"database_url": "postgresql://localhost/db"}"#)
                .expect("a config with only a URL is valid");

        assert!(!config.worker_pool_pre_ping);
        assert!(config.pool_pre_ping);
        let task_send = config.with_exact_pool(connections(2));
        assert!(task_send.pool_pre_ping);
        assert_eq!((task_send.pool_size, task_send.max_overflow), (2, 0));
        let worker = config.worker_process_config(connections(7));
        assert!(!worker.pool_pre_ping);
        assert_eq!((worker.pool_size, worker.max_overflow), (7, 0));
    }

    /// Python `test_app_config.py::TestPostgresConfigPgBouncer::test_the_worker_process_pre_pings_when_set`.
    #[test]
    fn the_worker_process_pre_pings_when_set() {
        let config: PostgresConfig = serde_json::from_str(
            r#"{
                "database_url": "postgresql://localhost/db",
                "pool_pre_ping": false,
                "worker_pool_pre_ping": true
            }"#,
        )
        .expect("both pre-ping fields are valid");

        assert!(!config.pool_pre_ping);
        assert!(!config.with_exact_pool(connections(2)).pool_pre_ping);
        let worker = config.worker_process_config(connections(7));
        assert!(worker.pool_pre_ping);
        assert_eq!((worker.pool_size, worker.max_overflow), (7, 0));
    }

    /// Python `test_app_config.py::TestPostgresConfigPgBouncer::test_a_removed_worker_pool_field_is_refused`:
    /// any value is refused, `null` included, with `CONFIG_INVALID_BROKER_POOL`
    /// and a help that names the pool of the worker process.
    #[test]
    fn a_removed_worker_pool_field_is_refused_by_name() {
        let cases = [
            (
                RemovedPoolField::WorkerPoolSize,
                "the worker process opens its pool with the resolved pool.max_size of its \
                 worker profile",
            ),
            (
                RemovedPoolField::WorkerMaxOverflow,
                "the pool of the worker process has no overflow: it holds at most \
                 pool.max_size connections",
            ),
        ];
        assert_eq!(ErrorCode::ConfigInvalidBrokerPool.to_string(), "HRS-213");
        assert!(ErrorCode::ConfigInvalidBrokerPool.is_config_error());
        for (field, note) in cases {
            let refusal = field.refusal();
            assert_eq!(refusal.code, Some(ErrorCode::ConfigInvalidBrokerPool));
            assert_eq!(
                refusal.message,
                format!("{} was removed in 0.7.0", field.key())
            );
            assert_eq!(
                refusal.notes,
                [
                    note,
                    "task code in a worker child that sends opens the task_send_pool of its \
                     child kind, not this pool",
                ],
            );
            let help = refusal.help_text.unwrap_or_default();
            assert!(
                help.contains("WorkerProfile(pool=ParentPool(max_size=PoolSize(connections=N)))"),
                "{help}",
            );

            for value in ["3", "0", "null"] {
                let json = format!(
                    r#"{{"database_url": "postgresql://localhost/db", "{}": {value}}}"#,
                    field.key(),
                );
                let error = serde_json::from_str::<PostgresConfig>(&json)
                    .expect_err("a removed pool field is refused");
                let text = error.to_string();
                assert!(
                    text.contains(&format!(
                        "error[HRS-213]: {} was removed in 0.7.0",
                        field.key()
                    )),
                    "{text}",
                );
                assert!(!text.contains("unknown field"), "{text}");
            }
        }
    }

    /// Python `test_app_config.py::TestPostgresConfigPgBouncer::test_a_removed_child_pool_field_is_refused`:
    /// the help names the pool argument of each child kind.
    #[test]
    fn a_removed_child_pool_field_is_refused_by_name() {
        let cases = [
            (RemovedPoolField::WorkerChildPoolMinSize, "1", "min_size=N"),
            (
                RemovedPoolField::WorkerChildPoolMaxSize,
                "4",
                "max_size=PoolSize(connections=N)",
            ),
            (
                RemovedPoolField::WorkerChildPoolCheck,
                "false",
                "pre_ping=False",
            ),
        ];
        for (field, value, pool_argument) in cases {
            let refusal = field.refusal();
            assert_eq!(refusal.code, Some(ErrorCode::ConfigInvalidBrokerPool));
            assert_eq!(
                refusal.message,
                format!("{} was removed in 0.7.0", field.key())
            );
            assert_eq!(
                refusal.notes,
                ["each child kind of a worker profile has its own connection pool"],
            );
            let help = refusal.help_text.unwrap_or_default();
            for successor in [
                format!("IsolatedChildren(pool=IsolatedPool({pool_argument}))"),
                format!("SharedAsyncChildren(pool=SharedPool({pool_argument}))"),
                format!("SharedThreadChildren(pool=SharedPool({pool_argument}))"),
            ] {
                assert!(help.contains(&successor), "{help}");
            }

            let json = format!(
                r#"{{"database_url": "postgresql://localhost/db", "{}": {value}}}"#,
                field.key(),
            );
            let error = serde_json::from_str::<PostgresConfig>(&json)
                .expect_err("a removed pool field is refused");
            assert!(
                error.to_string().contains(&format!(
                    "error[HRS-213]: {} was removed in 0.7.0",
                    field.key()
                )),
                "{error}",
            );
        }
    }

    /// Python `test_app_config.py::TestPostgresConfigPgBouncer::test_every_removed_pool_field_is_reported_at_once`.
    #[test]
    fn every_removed_pool_field_is_reported_at_once() {
        let config = serde_json::json!({
            "database_url": "postgresql://localhost/db",
            "worker_pool_size": 3,
            "worker_max_overflow": 2,
            "worker_child_pool_max_size": 2,
        });
        let fields = config.as_object().expect("the config is an object");

        let refusal =
            removed_pool_field_refusal(fields).expect_err("three removed fields are refused");

        assert_eq!(refusal.code, Some(ErrorCode::ConfigInvalidBrokerPool));
        assert_eq!(
            first_lines(&refusal),
            [
                "error[HRS-213]: worker_child_pool_max_size was removed in 0.7.0",
                "error[HRS-213]: worker_pool_size was removed in 0.7.0",
                "error[HRS-213]: worker_max_overflow was removed in 0.7.0",
            ],
        );
        let error = serde_json::from_value::<PostgresConfig>(config)
            .expect_err("three removed fields are refused");
        assert!(
            error
                .to_string()
                .contains("error[HRS-213]: aborting due to 3 previous errors"),
            "{error}",
        );
    }

    /// Python `test_app_config.py::TestPostgresConfigPgBouncer::test_every_removed_child_pool_field_is_reported_at_once`.
    #[test]
    fn every_removed_child_pool_field_is_reported_at_once() {
        let config = serde_json::json!({
            "database_url": "postgresql://localhost/db",
            "worker_child_pool_check": true,
            "worker_child_pool_max_size": 2,
            "worker_child_pool_min_size": 0,
        });
        let fields = config.as_object().expect("the config is an object");

        let refusal =
            removed_pool_field_refusal(fields).expect_err("three removed fields are refused");

        assert_eq!(
            first_lines(&refusal),
            [
                "error[HRS-213]: worker_child_pool_min_size was removed in 0.7.0",
                "error[HRS-213]: worker_child_pool_max_size was removed in 0.7.0",
                "error[HRS-213]: worker_child_pool_check was removed in 0.7.0",
            ],
        );
    }

    /// Python `test_app_config.py::TestPostgresConfigPgBouncer::test_the_removed_worker_pool_fields_are_no_fields`
    /// and `::test_the_removed_child_pool_fields_are_no_fields`: the refusal
    /// list holds the five fields of 0.6, and none is a field of the config.
    #[test]
    fn the_removed_pool_fields_are_no_fields() {
        let keys: Vec<&str> = RemovedPoolField::ALL
            .into_iter()
            .map(RemovedPoolField::key)
            .collect();
        assert_eq!(
            keys,
            [
                "worker_child_pool_min_size",
                "worker_child_pool_max_size",
                "worker_child_pool_check",
                "worker_pool_size",
                "worker_max_overflow",
            ],
        );
        let serialized = serde_json::to_value(PostgresConfig::from_database_url(
            "postgresql://localhost/db",
        ))
        .expect("the config serializes");
        let serialized = serialized.as_object().expect("the config is an object");
        for key in keys {
            assert!(!serialized.contains_key(key), "{key} is still a field");
        }
    }

    /// No removed field: the config parses.
    #[test]
    fn a_config_without_removed_fields_is_not_refused() {
        let config = serde_json::json!({"database_url": "postgresql://localhost/db"});
        let fields = config.as_object().expect("the config is an object");

        assert!(removed_pool_field_refusal(fields).is_ok());
    }

    #[test]
    fn invalid_session_url_scheme() {
        let mut config = PostgresConfig::from_database_url("postgresql://user:pass@pooler/db");
        config.session_database = SessionDatabase::Separate {
            url: "mysql://user:pass@direct/db".to_owned(),
        };

        assert!(matches!(
            config.validate(),
            Err(PostgresConfigError::InvalidSessionUrlScheme(_)),
        ));
    }

    /// The SQLAlchemy form of Python 0.7.0 is refused, and the refusal names
    /// the fix.
    #[test]
    fn a_driver_suffix_scheme_is_refused_with_the_fix() {
        let config =
            PostgresConfig::from_database_url("postgresql+psycopg://user:secret@localhost/db");

        let error = config.validate().unwrap_err();

        assert_eq!(
            error,
            PostgresConfigError::InvalidUrlScheme("postgresql+psycopg".to_owned()),
        );
        let message = error.to_string();
        assert!(
            message.contains("write 'postgresql://' instead"),
            "{message}"
        );
        assert!(!message.contains("secret"), "{message}");
    }

    #[test]
    fn a_driver_suffix_session_scheme_is_refused() {
        let mut config = PostgresConfig::from_database_url("postgresql://user:pass@pooler/db");
        config.session_database = SessionDatabase::Separate {
            url: "postgres+asyncpg://user:pass@direct/db".to_owned(),
        };

        assert_eq!(
            config.validate(),
            Err(PostgresConfigError::InvalidSessionUrlScheme(
                "postgres+asyncpg".to_owned()
            )),
        );
    }

    /// A URL with no `://` is reported without its text, which can hold a
    /// password.
    #[test]
    fn a_url_without_a_scheme_is_refused_without_echoing_it() {
        let config = PostgresConfig::from_database_url("user:secret@localhost/db");

        let error = config.validate().unwrap_err();

        assert_eq!(
            error,
            PostgresConfigError::InvalidUrlScheme(NO_URL_SCHEME.to_owned()),
        );
        assert!(!error.to_string().contains("secret"));
    }

    /// Python `test_app_config.py::TestPostgresConfigPgBouncer::test_a_removed_worker_pool_field_is_refused`
    /// and `::test_a_removed_child_pool_field_is_refused`: a pool field that
    /// moved to the worker profile is refused by name, not as an unknown key.
    #[test]
    fn a_removed_pool_field_is_refused_by_name() {
        for field in [
            "worker_pool_size",
            "worker_max_overflow",
            "worker_child_pool_min_size",
            "worker_child_pool_max_size",
            "worker_child_pool_check",
        ] {
            let json = format!(r#"{{"database_url": "postgresql://localhost/db", "{field}": 3}}"#);
            let error = serde_json::from_str::<PostgresConfig>(&json)
                .expect_err("a removed pool field must be refused");
            assert!(
                error
                    .to_string()
                    .contains(&format!("{field} was removed in 0.7.0")),
                "{error}",
            );
        }
    }
}

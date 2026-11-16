use serde::{Deserialize, Serialize};

use crate::core::config::socket::{socket_settings, TcpKeepalivesConfig, TcpUserTimeoutConfig};
use crate::db::SocketSettings;

/// PostgreSQL broker configuration.
///
/// `Debug` is implemented manually to redact the password in the connection
/// URLs so structured logs / panics never leak credentials. Parity with
/// horsies PR #101 f368965c.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    /// PostgreSQL runtime connection URL.
    ///
    /// This may point at a PgBouncer transaction-pool endpoint when
    /// `pgbouncer_transaction_mode` is true.
    pub database_url: String,

    /// PostgreSQL session connection URL for schema initialization,
    /// partition maintenance, and LISTEN/NOTIFY.
    ///
    /// PgBouncer transaction pooling cannot preserve session state for
    /// LISTEN/NOTIFY, so pooled runtime deployments must provide a direct or
    /// session-pooled URL here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_database_url: Option<String>,

    /// Configure the runtime pool for PgBouncer transaction pooling.
    ///
    /// SQLx uses protocol-level prepared statements for parameterized
    /// Postgres queries. The PgBouncer endpoint must therefore have prepared
    /// statement tracking enabled (`max_prepared_statements > 0`). This flag
    /// disables SQLx's local statement cache and requires `session_database_url`
    /// so schema initialization and LISTEN/NOTIFY continue to use a
    /// session-capable connection.
    #[serde(default)]
    pub pgbouncer_transaction_mode: bool,

    /// Whether to pre-ping the connection pool.
    #[serde(default = "default_true")]
    pub pool_pre_ping: bool,

    /// Connection pool size.
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,

    /// Maximum overflow connections beyond pool_size.
    #[serde(default = "default_max_overflow")]
    pub max_overflow: u32,

    /// Default policy for retaining canonical rerun input at enqueue.
    #[serde(default)]
    pub retain_rerun_input_default: bool,

    /// Timeout in seconds for acquiring a connection from the pool.
    #[serde(default = "default_pool_timeout")]
    pub pool_timeout: u32,

    /// Seconds before a connection is recycled.
    #[serde(default = "default_pool_recycle")]
    pub pool_recycle: u32,

    /// Whether to log SQL statements.
    #[serde(default)]
    pub echo: bool,

    /// TCP keepalives of every core connection (Python `tcp_keepalives`,
    /// `tcp_keepalives_idle`, `tcp_keepalives_interval`,
    /// `tcp_keepalives_count`). Default: on, 30 s, 10 s, 3 probes.
    #[serde(default)]
    pub tcp_keepalives: TcpKeepalivesConfig,

    /// TCP user timeout of every core connection (Python
    /// `tcp_user_timeout_ms`; its 0 is `SystemDefault`). Default: 30,000 ms.
    #[serde(default)]
    pub tcp_user_timeout: TcpUserTimeoutConfig,
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
            .field(
                "session_database_url",
                &self.session_database_url.as_deref().map(mask_url),
            )
            .field(
                "pgbouncer_transaction_mode",
                &self.pgbouncer_transaction_mode,
            )
            .field("pool_pre_ping", &self.pool_pre_ping)
            .field("pool_size", &self.pool_size)
            .field("max_overflow", &self.max_overflow)
            .field(
                "retain_rerun_input_default",
                &self.retain_rerun_input_default,
            )
            .field("pool_timeout", &self.pool_timeout)
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
    30
}
fn default_max_overflow() -> u32 {
    30
}
fn default_pool_timeout() -> u32 {
    30
}
fn default_pool_recycle() -> u32 {
    1800
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
    /// `session_database_url` does not start with `postgresql://` or
    /// `postgres://`. The value is the scheme only, as in
    /// [`PostgresConfigError::InvalidUrlScheme`].
    #[error(
        "invalid session_database_url scheme {0:?}: the URL must start with 'postgresql://' \
         or 'postgres://'; a driver suffix such as 'postgresql+psycopg://' is refused, \
         write 'postgresql://' instead"
    )]
    InvalidSessionUrlScheme(String),
    #[error("pgbouncer_transaction_mode=true requires session_database_url for schema initialization, partition maintenance, and LISTEN/NOTIFY")]
    MissingSessionDatabaseUrl,
    #[error(
        "session_database_url must not use the transaction-pool URL: partition maintenance \
         and LISTEN require PostgreSQL session affinity; use a direct or session-capable \
         URL for session_database_url"
    )]
    SessionUrlIsTransactionPoolUrl,
    /// The driver cannot parse a URL whose scheme passed. The value is the
    /// driver's reason, which does not hold the URL.
    #[error("database URL cannot be parsed: {0}")]
    UnparseableUrl(String),
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
    pub fn from_url(url: impl Into<String>) -> Self {
        Self {
            database_url: url.into(),
            session_database_url: None,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: default_true(),
            pool_size: default_pool_size(),
            max_overflow: default_max_overflow(),
            retain_rerun_input_default: false,
            pool_timeout: default_pool_timeout(),
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
    /// must be a direct or session-pooled endpoint used for schema work,
    /// partition maintenance, and LISTEN/NOTIFY. The transaction-pool endpoint must support
    /// protocol-level prepared statement tracking (`max_prepared_statements > 0`).
    pub fn from_pgbouncer_urls(
        database_url: impl Into<String>,
        session_database_url: impl Into<String>,
    ) -> Self {
        let mut config = Self::from_url(database_url);
        config.session_database_url = Some(session_database_url.into());
        config.pgbouncer_transaction_mode = true;
        config
    }

    /// Return the URL used for schema initialization, partition maintenance,
    /// and LISTEN/NOTIFY.
    pub fn effective_session_database_url(&self) -> &str {
        self.session_database_url
            .as_deref()
            .unwrap_or(&self.database_url)
    }

    /// Validate the configuration.
    ///
    /// Accepts only the `postgresql://` and `postgres://` schemes for both
    /// URLs. In PgBouncer transaction mode, requires a session URL that
    /// differs from `database_url`.
    pub fn validate(&self) -> Result<(), PostgresConfigError> {
        if !is_postgres_url(&self.database_url) {
            return Err(PostgresConfigError::InvalidUrlScheme(url_scheme(
                &self.database_url,
            )));
        }
        if let Some(session_url) = &self.session_database_url {
            if !is_postgres_url(session_url) {
                return Err(PostgresConfigError::InvalidSessionUrlScheme(url_scheme(
                    session_url,
                )));
            }
        }
        if self.pgbouncer_transaction_mode && self.session_database_url.is_none() {
            return Err(PostgresConfigError::MissingSessionDatabaseUrl);
        }
        if self.pgbouncer_transaction_mode
            && self.session_database_url.as_deref() == Some(self.database_url.as_str())
        {
            return Err(PostgresConfigError::SessionUrlIsTransactionPoolUrl);
        }
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
            session_database_url: Some("postgresql://user:topsecret@localhost/db".to_owned()),
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            pool_size: 30,
            max_overflow: 10,
            retain_rerun_input_default: false,
            pool_timeout: 30,
            pool_recycle: 1800,
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
            session_database_url: None,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            pool_size: 30,
            max_overflow: 30,
            retain_rerun_input_default: false,
            pool_timeout: 30,
            pool_recycle: 1800,
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
            session_database_url: None,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            pool_size: 30,
            max_overflow: 30,
            retain_rerun_input_default: false,
            pool_timeout: 30,
            pool_recycle: 1800,
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
            session_database_url: None,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            pool_size: 30,
            max_overflow: 30,
            retain_rerun_input_default: false,
            pool_timeout: 30,
            pool_recycle: 1800,
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
        assert_eq!(config.pool_size, 30);
        assert_eq!(config.max_overflow, 30);
        assert!(config.pool_pre_ping);
        assert!(!config.echo);
        assert!(config.session_database_url.is_none());
        assert!(!config.pgbouncer_transaction_mode);
        assert!(!config.retain_rerun_input_default);
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
        let mut config = PostgresConfig::from_url("postgresql://user:pass@pooler/db");
        config.pgbouncer_transaction_mode = true;

        assert!(matches!(
            config.validate(),
            Err(PostgresConfigError::MissingSessionDatabaseUrl),
        ));
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
        let mut direct = PostgresConfig::from_url("postgresql://user:pass@direct/db");
        direct.session_database_url = Some("postgresql://user:pass@direct/db".to_owned());
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

    #[test]
    fn invalid_session_url_scheme() {
        let mut config = PostgresConfig::from_url("postgresql://user:pass@pooler/db");
        config.session_database_url = Some("mysql://user:pass@direct/db".to_owned());

        assert!(matches!(
            config.validate(),
            Err(PostgresConfigError::InvalidSessionUrlScheme(_)),
        ));
    }

    /// The SQLAlchemy form of Python 0.7.0 is refused, and the refusal names
    /// the fix.
    #[test]
    fn a_driver_suffix_scheme_is_refused_with_the_fix() {
        let config = PostgresConfig::from_url("postgresql+psycopg://user:secret@localhost/db");

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
        let mut config = PostgresConfig::from_url("postgresql://user:pass@pooler/db");
        config.session_database_url = Some("postgres+asyncpg://user:pass@direct/db".to_owned());

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
        let config = PostgresConfig::from_url("user:secret@localhost/db");

        let error = config.validate().unwrap_err();

        assert_eq!(
            error,
            PostgresConfigError::InvalidUrlScheme(NO_URL_SCHEME.to_owned()),
        );
        assert!(!error.to_string().contains("secret"));
    }
}

use serde::{Deserialize, Serialize};

/// PostgreSQL broker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// PostgreSQL runtime connection URL.
    ///
    /// This may point at a PgBouncer transaction-pool endpoint when
    /// `pgbouncer_transaction_mode` is true.
    pub database_url: String,

    /// PostgreSQL session connection URL for schema initialization and
    /// LISTEN/NOTIFY.
    ///
    /// PgBouncer transaction pooling cannot preserve session state for
    /// LISTEN/NOTIFY, so pooled runtime deployments must provide a direct or
    /// session-pooled URL here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_database_url: Option<String>,

    /// Disable SQLx's named prepared-statement cache for PgBouncer
    /// transaction-pool compatibility.
    ///
    /// Requires `session_database_url` so schema initialization and
    /// LISTEN/NOTIFY continue to use a session-capable connection.
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

    /// Timeout in seconds for acquiring a connection from the pool.
    #[serde(default = "default_pool_timeout")]
    pub pool_timeout: u32,

    /// Seconds before a connection is recycled.
    #[serde(default = "default_pool_recycle")]
    pub pool_recycle: u32,

    /// Whether to log SQL statements.
    #[serde(default)]
    pub echo: bool,
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
#[derive(Debug, Clone, thiserror::Error)]
pub enum PostgresConfigError {
    #[error("invalid database URL scheme: expected 'postgresql://' or 'postgres://', got '{0}'")]
    InvalidUrlScheme(String),
    #[error(
        "invalid session_database_url scheme: expected 'postgresql://' or 'postgres://', got '{0}'"
    )]
    InvalidSessionUrlScheme(String),
    #[error("pgbouncer_transaction_mode=true requires session_database_url for schema initialization and LISTEN/NOTIFY")]
    MissingSessionDatabaseUrl,
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
            pool_timeout: default_pool_timeout(),
            pool_recycle: default_pool_recycle(),
            echo: false,
        }
    }

    /// Create a PostgresConfig for PgBouncer transaction pooling.
    ///
    /// `database_url` should be the transaction-pool endpoint. `session_database_url`
    /// must be a direct or session-pooled endpoint used for schema work and
    /// LISTEN/NOTIFY.
    pub fn from_pgbouncer_urls(
        database_url: impl Into<String>,
        session_database_url: impl Into<String>,
    ) -> Self {
        let mut config = Self::from_url(database_url);
        config.session_database_url = Some(session_database_url.into());
        config.pgbouncer_transaction_mode = true;
        config
    }

    /// Return the URL used for schema initialization and LISTEN/NOTIFY.
    pub fn effective_session_database_url(&self) -> &str {
        self.session_database_url
            .as_deref()
            .unwrap_or(&self.database_url)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), PostgresConfigError> {
        if !is_postgres_url(&self.database_url) {
            let scheme = self
                .database_url
                .split("://")
                .next()
                .unwrap_or(&self.database_url[..20.min(self.database_url.len())]);
            return Err(PostgresConfigError::InvalidUrlScheme(scheme.to_owned()));
        }
        if let Some(session_url) = &self.session_database_url {
            if !is_postgres_url(session_url) {
                let scheme = session_url
                    .split("://")
                    .next()
                    .unwrap_or(&session_url[..20.min(session_url.len())]);
                return Err(PostgresConfigError::InvalidSessionUrlScheme(
                    scheme.to_owned(),
                ));
            }
        }
        if self.pgbouncer_transaction_mode && self.session_database_url.is_none() {
            return Err(PostgresConfigError::MissingSessionDatabaseUrl);
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
    fn valid_url() {
        let config = PostgresConfig {
            database_url: "postgresql://user:pass@localhost/db".to_owned(),
            session_database_url: None,
            pgbouncer_transaction_mode: false,
            pool_pre_ping: true,
            pool_size: 30,
            max_overflow: 30,
            pool_timeout: 30,
            pool_recycle: 1800,
            echo: false,
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
            pool_timeout: 30,
            pool_recycle: 1800,
            echo: false,
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
            pool_timeout: 30,
            pool_recycle: 1800,
            echo: false,
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

    #[test]
    fn invalid_session_url_scheme() {
        let mut config = PostgresConfig::from_url("postgresql://user:pass@pooler/db");
        config.session_database_url = Some("mysql://user:pass@direct/db".to_owned());

        assert!(matches!(
            config.validate(),
            Err(PostgresConfigError::InvalidSessionUrlScheme(_)),
        ));
    }
}

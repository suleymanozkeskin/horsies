use serde::{Deserialize, Serialize};

/// PostgreSQL broker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// PostgreSQL connection URL.
    pub database_url: String,

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
}

impl PostgresConfig {
    /// Create a PostgresConfig from just a database URL, using defaults for
    /// all other fields.
    ///
    /// This mirrors the Python convenience of `PostgresConfig(database_url="...")`.
    pub fn from_url(url: impl Into<String>) -> Self {
        Self {
            database_url: url.into(),
            pool_pre_ping: default_true(),
            pool_size: default_pool_size(),
            max_overflow: default_max_overflow(),
            pool_timeout: default_pool_timeout(),
            pool_recycle: default_pool_recycle(),
            echo: false,
        }
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), PostgresConfigError> {
        if !self.database_url.starts_with("postgresql://")
            && !self.database_url.starts_with("postgres://")
        {
            let scheme = self
                .database_url
                .split("://")
                .next()
                .unwrap_or(&self.database_url[..20.min(self.database_url.len())]);
            return Err(PostgresConfigError::InvalidUrlScheme(scheme.to_owned()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_url() {
        let config = PostgresConfig {
            database_url: "postgresql://user:pass@localhost/db".to_owned(),
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
    }
}

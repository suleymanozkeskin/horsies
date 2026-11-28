//! The one builder of a producer manifest from the broker settings of an
//! app (`PostgresConfig`).
//!
//! The Rust client and the TypeScript addon resolve their app config through
//! this module. Python builds the same manifest from its own config types and
//! uses the same reservation window value ([`ReservationWindowMicros::DEFAULT`]).

use crate::core::config::socket::input::SocketValueRefused;
use crate::core::error::HorsiesErrorCode;
use crate::core::{PostgresConfig, SessionDatabase as ConfigSessionDatabase};

use super::{
    DatabaseManifest, DatabaseUrl, ManifestVersion, PoolConnections, PoolSize, ProducerManifest,
    ReservationWindowMicros, SchemaMode, SendManifest, SessionDatabase, WholeSeconds,
};

/// The URL scheme that the core's connection layer reads.
const CORE_URL_SCHEME: &str = "postgresql://";

/// The short form of [`CORE_URL_SCHEME`] that the config also admits.
const SHORT_URL_SCHEME: &str = "postgres://";

/// A broker setting that passes `PostgresConfig::validate` but is outside the
/// bounds of the manifest, or a socket value outside its bound.
///
/// The cause names the bound, never the value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BrokerSettingRefused {
    /// `pool_size` is outside 1 to [`super::MAX_POOL_CONNECTIONS`].
    #[error("database.pool_size: {cause}")]
    PoolSize { cause: String },
    /// `max_overflow` is above [`super::MAX_POOL_CONNECTIONS`].
    #[error("database.max_overflow: {cause}")]
    MaxOverflow { cause: String },
    /// A TCP keepalive or user timeout value is outside its bound.
    /// `PostgresConfig::validate` refuses the same value.
    #[error("database.{0}")]
    Socket(SocketValueRefused),
}

impl BrokerSettingRefused {
    /// The `PostgresConfig` field that holds the refused value.
    pub fn field(&self) -> &'static str {
        match self {
            Self::PoolSize { .. } => "pool_size",
            Self::MaxOverflow { .. } => "max_overflow",
            Self::Socket(refused) => refused.field,
        }
    }

    /// The bound that the value breaks.
    pub fn cause(&self) -> &str {
        match self {
            Self::PoolSize { cause } | Self::MaxOverflow { cause } => cause,
            Self::Socket(refused) => &refused.cause,
        }
    }

    /// The catalog code of the refusal.
    pub fn code(&self) -> HorsiesErrorCode {
        match self {
            Self::PoolSize { .. } | Self::MaxOverflow { .. } => {
                HorsiesErrorCode::ConfigInvalidDatabasePool
            }
            Self::Socket(_) => HorsiesErrorCode::ConfigInvalidKeepalive,
        }
    }
}

/// The producer manifest of the broker settings `config` with the schema
/// mode `schema`, or every refusal, in field order: `pool_size`,
/// `max_overflow`, the keepalives, the user timeout.
///
/// Pure: no I/O. It does not run `PostgresConfig::validate`; the refusals
/// are the bounds that the manifest adds to it, and the socket bounds that
/// it shares. The send section holds the default reservation window
/// ([`ReservationWindowMicros::DEFAULT`]).
pub fn producer_manifest(
    config: &PostgresConfig,
    schema: SchemaMode,
) -> Result<ProducerManifest, Vec<BrokerSettingRefused>> {
    Ok(ProducerManifest {
        manifest_version: ManifestVersion::CURRENT,
        database: database_manifest(config)?,
        schema,
        send: send_manifest(config),
    })
}

/// The send section of a manifest: the rerun-input default of `config` and
/// the default reservation window. Pure.
pub fn send_manifest(config: &PostgresConfig) -> SendManifest {
    SendManifest {
        retain_rerun_input_default: config.retain_rerun_input_default,
        idempotency_reservation_window: ReservationWindowMicros::DEFAULT,
    }
}

/// The manifest form of the session database of `config`: the main URL, or
/// the separate URL with the scheme that the core reads. Pure.
pub fn session_database_manifest(config: &PostgresConfig) -> SessionDatabase {
    match &config.session_database {
        ConfigSessionDatabase::SameAsDatabaseUrl => SessionDatabase::SameAsDatabaseUrl,
        ConfigSessionDatabase::Separate { url } => SessionDatabase::Separate {
            url: core_database_url(url),
        },
    }
}

/// `url` with the scheme that the core reads: `postgres://` becomes
/// `postgresql://` and the rest is kept (Python `core_database_url`).
/// `PostgresConfig::validate` admits only these two schemes. Pure.
pub fn core_scheme_url(url: &str) -> String {
    match url.strip_prefix(SHORT_URL_SCHEME) {
        Some(rest) => format!("{CORE_URL_SCHEME}{rest}"),
        None => url.to_owned(),
    }
}

/// [`core_scheme_url`] as a manifest URL. Pure.
pub fn core_database_url(url: &str) -> DatabaseUrl {
    DatabaseUrl::new(core_scheme_url(url))
}

fn database_manifest(
    config: &PostgresConfig,
) -> Result<DatabaseManifest, Vec<BrokerSettingRefused>> {
    let pool_size = PoolSize::try_from(config.pool_size)
        .map_err(|cause| BrokerSettingRefused::PoolSize { cause });
    let max_overflow = PoolConnections::try_from(config.max_overflow)
        .map_err(|cause| BrokerSettingRefused::MaxOverflow { cause });
    let keepalives = config
        .tcp_keepalives
        .checked()
        .map_err(BrokerSettingRefused::Socket);
    let user_timeout = config
        .tcp_user_timeout
        .checked()
        .map_err(BrokerSettingRefused::Socket);
    match (pool_size, max_overflow, keepalives, user_timeout) {
        (Ok(pool_size), Ok(max_overflow), Ok(tcp_keepalives), Ok(tcp_user_timeout)) => {
            Ok(DatabaseManifest {
                database_url: core_database_url(&config.database_url),
                session_database: session_database_manifest(config),
                pgbouncer_transaction_mode: config.pgbouncer_transaction_mode,
                pool_pre_ping: config.pool_pre_ping,
                pool_size,
                max_overflow,
                pool_timeout_seconds: WholeSeconds::from_secs(config.pool_timeout_seconds),
                pool_recycle: config.pool_recycle,
                echo: config.echo,
                tcp_keepalives,
                tcp_user_timeout,
            })
        }
        (pool_size, max_overflow, keepalives, user_timeout) => Err(pool_size
            .err()
            .into_iter()
            .chain(max_overflow.err())
            .chain(keepalives.err())
            .chain(user_timeout.err())
            .collect()),
    }
}

#[cfg(test)]
mod tests {
    use crate::core::config::socket::input::{TcpKeepalivesConfig, TcpUserTimeoutConfig};
    use crate::core::history::identity::keys::IDEMPOTENCY_WINDOW_DEFAULT_MICROS;
    use crate::manifest::{parse_producer_manifest, PoolRecycle};

    use super::*;

    const ONE_DAY_MICROS: u64 = 86_400_000_000;

    fn broker(url: &str) -> PostgresConfig {
        PostgresConfig::from_database_url(url)
    }

    #[test]
    fn the_default_window_is_one_day_and_the_window_of_the_broker() {
        assert_eq!(ReservationWindowMicros::DEFAULT.get(), ONE_DAY_MICROS);
        assert_eq!(
            i64::try_from(ReservationWindowMicros::DEFAULT.get()),
            Ok(IDEMPOTENCY_WINDOW_DEFAULT_MICROS)
        );
        assert_eq!(
            ReservationWindowMicros::try_from(ONE_DAY_MICROS),
            Ok(ReservationWindowMicros::DEFAULT)
        );
    }

    #[test]
    fn the_core_parser_reads_the_built_manifest_back() {
        for schema in [
            SchemaMode::MigrateAndValidate,
            SchemaMode::ObserveOnly,
            SchemaMode::AssumeInitialized,
        ] {
            let manifest =
                producer_manifest(&broker("postgresql://app@db.example/horsies"), schema)
                    .expect("the settings are inside the bounds");
            let text = serde_json::to_string(&manifest).expect("a manifest writes as JSON");
            assert_eq!(parse_producer_manifest(&text), Ok(manifest.clone()));
            assert_eq!(manifest.schema, schema);
        }
    }

    #[test]
    fn the_manifest_holds_the_config_values() {
        let config = PostgresConfig {
            session_database: ConfigSessionDatabase::Separate {
                url: "postgres://app@session.example/horsies".to_owned(),
            },
            pgbouncer_transaction_mode: true,
            pool_size: 7,
            max_overflow: 0,
            pool_timeout_seconds: 0,
            pool_recycle: PoolRecycle::Never,
            retain_rerun_input_default: true,
            tcp_keepalives: TcpKeepalivesConfig::Off,
            tcp_user_timeout: TcpUserTimeoutConfig::SystemDefault,
            ..broker("postgres://app@pool.example/horsies")
        };
        let manifest = producer_manifest(&config, SchemaMode::MigrateAndValidate)
            .expect("the settings are inside the bounds");
        let database = &manifest.database;
        assert_eq!(
            database.database_url.expose(),
            "postgresql://app@pool.example/horsies"
        );
        assert_eq!(
            database.session_database,
            SessionDatabase::Separate {
                url: core_database_url("postgresql://app@session.example/horsies")
            }
        );
        assert!(database.pgbouncer_transaction_mode);
        assert_eq!(database.pool_size.get(), 7);
        assert_eq!(database.max_overflow.get(), 0);
        assert_eq!(database.pool_timeout_seconds.get(), 0);
        assert_eq!(database.pool_recycle, PoolRecycle::Never);
        assert!(manifest.send.retain_rerun_input_default);
        assert_eq!(
            manifest.send.idempotency_reservation_window,
            ReservationWindowMicros::DEFAULT
        );
    }

    #[test]
    fn the_short_scheme_becomes_the_core_scheme() {
        assert_eq!(
            core_database_url("postgres://app:secret@db/horsies").expose(),
            "postgresql://app:secret@db/horsies"
        );
        assert_eq!(
            core_database_url("postgresql://app@db/horsies").expose(),
            "postgresql://app@db/horsies"
        );
    }

    #[test]
    fn a_pool_value_outside_the_manifest_bounds_is_refused_by_field_with_its_code() {
        for (config, field) in [
            (
                PostgresConfig {
                    pool_size: 0,
                    ..broker("postgresql://db/horsies")
                },
                "pool_size",
            ),
            (
                PostgresConfig {
                    max_overflow: 10_001,
                    ..broker("postgresql://db/horsies")
                },
                "max_overflow",
            ),
        ] {
            let refused = producer_manifest(&config, SchemaMode::MigrateAndValidate)
                .expect_err("the value is outside the bounds");
            assert_eq!(refused.len(), 1);
            assert_eq!(refused[0].field(), field);
            assert_eq!(
                refused[0].code(),
                HorsiesErrorCode::ConfigInvalidDatabasePool
            );
            assert!(refused[0]
                .to_string()
                .starts_with(&format!("database.{field}: ")));
        }
    }

    #[test]
    fn a_socket_value_outside_its_bound_is_refused_with_the_keepalive_code() {
        let config = PostgresConfig {
            tcp_user_timeout: TcpUserTimeoutConfig::After { milliseconds: 0 },
            ..broker("postgresql://db/horsies")
        };
        let refused = producer_manifest(&config, SchemaMode::MigrateAndValidate)
            .expect_err("zero milliseconds are outside the bound");
        assert_eq!(refused.len(), 1);
        assert_eq!(refused[0].field(), "tcp_user_timeout.milliseconds");
        assert_eq!(refused[0].code(), HorsiesErrorCode::ConfigInvalidKeepalive);
    }

    #[test]
    fn every_refusal_comes_in_field_order() {
        let config = PostgresConfig {
            pool_size: 0,
            max_overflow: 10_001,
            tcp_user_timeout: TcpUserTimeoutConfig::After { milliseconds: 0 },
            ..broker("postgresql://db/horsies")
        };
        let refused = producer_manifest(&config, SchemaMode::MigrateAndValidate)
            .expect_err("three values are outside their bounds");
        let fields: Vec<&str> = refused.iter().map(BrokerSettingRefused::field).collect();
        assert_eq!(
            fields,
            ["pool_size", "max_overflow", "tcp_user_timeout.milliseconds"]
        );
    }

    #[test]
    fn a_refusal_names_the_bound_and_never_the_value() {
        let config = PostgresConfig {
            pool_size: 123_456,
            ..broker("postgresql://db/horsies")
        };
        let refused = producer_manifest(&config, SchemaMode::MigrateAndValidate)
            .expect_err("the value is outside the bounds");
        assert!(!refused[0].cause().contains("123456"));
    }
}

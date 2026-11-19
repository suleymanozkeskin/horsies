//! The app's configuration, and its resolution into the core's manifests.
//!
//! `horsies-core` holds the one serde schema of the configuration, which the
//! Python client reads too, and its validation rules. This module names the
//! types a Rust app builds an [`AppConfig`] from, adds the rules that only a
//! Rust app has, and resolves a validated config into the manifest that the
//! core reads, as the Python client does (`clients/python/horsies/core/manifest.py`).

use std::collections::BTreeMap;

use horsies_core::manifest::{
    DatabaseManifest, DatabaseUrl, ManifestVersion, PoolConnections, PoolSize as PoolSizeValue,
    ProducerManifest, ReservationWindowMicros, SchemaMode, SendManifest, SessionDatabase,
    WholeSeconds,
};

use crate::{ErrorCode, HorsiesError};

mod scheduler_manifest;
mod worker_manifest;

pub(crate) use scheduler_manifest::{
    schedule_input_text, scheduler_manifest, ScheduleManifestRefused,
};
pub(crate) use worker_manifest::{
    rust_child_launch, task_classes, worker_manifest, ChildLaunchRefused, ManifestBoundRefused,
};

pub use horsies_core::core::config::bounded::{BoundedU32, OutOfBounds};
pub use horsies_core::core::config::worker_profile::{
    ClaimLimits, DrainTimeout, DrainTimeoutRefused, ExecutorThreadCount, ExecutorThreadsSetting,
    IsolatedChildCount, IsolatedChildren, IsolatedPool, IsolatedRecycle, IsolatedSetting,
    MemoryLimitSetting, MemoryMb, ParentPool, PoolConnectionCount, PoolMinSize, PoolSize,
    PoolSizeSetting, SharedAsyncChildCount, SharedAsyncChildren, SharedAsyncSeats,
    SharedAsyncSetting, SharedPool, SharedRecycle, SharedThreadChildCount, SharedThreadChildren,
    SharedThreadSeats, SharedThreadSetting, TaskCount, TaskLimitSetting,
};
pub use horsies_core::core::config::{
    ChildPreload, KeepaliveProbes, KeepaliveSeconds, ParentCpu, TcpKeepalivesConfig,
    TcpUserTimeoutConfig, TcpUserTimeoutMs, WorkerProfile,
};
pub use horsies_core::manifest::PoolRecycle;
pub use horsies_core::{
    mask_database_url, AppConfig, AppConfigError, CronEnumTerm, CronNumericTerm, CronOrdinal,
    CronSchedule, CustomQueueConfig, CustomQueueConfigError, DailySchedule, DaySelector,
    HourlySchedule, IntervalSchedule, JsonTextRefused, Month, MonthlySchedule, PausedAgeRefused,
    PausedWorkflowAge, PayloadPolicy, PostgresConfig, PostgresConfigError, QueueMode,
    RecoveryConfig, RecoveryConfigError, ResilienceConfigError, RetentionConfig,
    RetentionConfigError, ScheduleConfig, SchedulePattern, TaskSchedule, Weekday, WeeklySchedule,
    WorkerResilienceConfig,
};

/// The reservation window of a keyed send: 24 hours, in microseconds
/// (Python `IDEMPOTENCY_WINDOW_DEFAULT`, the window of every app-built
/// broker).
const DEFAULT_RESERVATION_WINDOW_MICROS: u64 = 24 * 60 * 60 * 1_000_000;

/// The URL scheme that the core's connection layer reads.
const CORE_URL_SCHEME: &str = "postgresql://";

/// The short form of [`CORE_URL_SCHEME`] that the config also admits.
const SHORT_URL_SCHEME: &str = "postgres://";

/// A broker setting that passes `PostgresConfig::validate` but is outside
/// the bounds of the core's manifest. The cause names the bound, never the
/// value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("broker.{field}: {cause}")]
pub(crate) struct BrokerSettingRefused {
    /// The `PostgresConfig` field.
    pub(crate) field: &'static str,
    /// The bound that the value breaks.
    pub(crate) cause: String,
}

impl BrokerSettingRefused {
    /// The config refusal. It has no catalog code: Python refuses the same
    /// values through field bounds, which carry none.
    pub(crate) fn to_horsies_error(&self) -> HorsiesError {
        HorsiesError::new(format!("invalid broker config: {self}"))
    }
}

/// The producer manifest of `config` with the schema mode `schema`.
///
/// Pure: no I/O. `config` passed `PostgresConfig::validate`. Refuses a pool
/// size outside 1 to 10,000 and an overflow above 10,000, which the
/// manifest bounds and the config validation does not.
pub(crate) fn producer_manifest(
    config: &PostgresConfig,
    schema: SchemaMode,
) -> Result<ProducerManifest, BrokerSettingRefused> {
    Ok(ProducerManifest {
        manifest_version: ManifestVersion::CURRENT,
        database: database_manifest(config)?,
        schema,
        send: send_manifest(config),
    })
}

/// The send section of a manifest: the rerun-input default of the config
/// and the reservation window of every app-built broker. Pure.
pub(crate) fn send_manifest(config: &PostgresConfig) -> SendManifest {
    SendManifest {
        retain_rerun_input_default: config.retain_rerun_input_default,
        idempotency_reservation_window: default_reservation_window(),
    }
}

fn database_manifest(config: &PostgresConfig) -> Result<DatabaseManifest, BrokerSettingRefused> {
    let pool_size =
        PoolSizeValue::try_from(config.pool_size).map_err(|cause| BrokerSettingRefused {
            field: "pool_size",
            cause,
        })?;
    let max_overflow =
        PoolConnections::try_from(config.max_overflow).map_err(|cause| BrokerSettingRefused {
            field: "max_overflow",
            cause,
        })?;
    Ok(DatabaseManifest {
        database_url: core_database_url(&config.database_url),
        session_database: session_database(config),
        pgbouncer_transaction_mode: config.pgbouncer_transaction_mode,
        pool_pre_ping: config.pool_pre_ping,
        pool_size,
        max_overflow,
        pool_timeout: WholeSeconds::from_secs(config.pool_timeout),
        pool_recycle: config.pool_recycle,
        echo: config.echo,
        tcp_keepalives: config.tcp_keepalives,
        tcp_user_timeout: config.tcp_user_timeout,
    })
}

/// No `session_database_url` is the main URL; a URL is the separate session
/// database, with the scheme that the core reads.
pub(crate) fn session_database(config: &PostgresConfig) -> SessionDatabase {
    match &config.session_database_url {
        None => SessionDatabase::SameAsDatabaseUrl,
        Some(url) => SessionDatabase::Separate {
            url: core_database_url(url),
        },
    }
}

/// `url` with the scheme that the core reads: `postgres://` becomes
/// `postgresql://`, and the rest is kept as it is (Python
/// `core_database_url`). `PostgresConfig::validate` admits only these two
/// schemes.
pub(crate) fn core_database_url(url: &str) -> DatabaseUrl {
    let text = match url.strip_prefix(SHORT_URL_SCHEME) {
        Some(rest) => format!("{CORE_URL_SCHEME}{rest}"),
        None => url.to_owned(),
    };
    // A `DatabaseUrl` is built only from its serde form, one JSON string, so
    // that no code path holds an unchecked URL type. A string always reads.
    serde_json::from_value(serde_json::Value::String(text))
        .expect("a JSON string reads as a DatabaseUrl")
}

fn default_reservation_window() -> ReservationWindowMicros {
    ReservationWindowMicros::try_from(DEFAULT_RESERVATION_WINDOW_MICROS)
        .expect("24 hours is inside the reservation window bounds of 1 us to 30 days")
}

/// The refusals of the rules that only a Rust app has, in profile name
/// order. Pure.
///
/// - A worker profile asks for no preload parent. A preload parent imports
///   Python code once before it forks children; a Rust worker starts each
///   child with `exec` of its own binary, which holds every task. A profile
///   that does not set `preload` has the core's default,
///   `ChildPreload::NoPreload`; an explicit `Library` or `App` is refused.
pub(crate) fn rust_config_refusals(config: &AppConfig) -> Vec<HorsiesError> {
    preload_refusals(&config.worker_profiles)
}

fn preload_refusals(profiles: &BTreeMap<String, WorkerProfile>) -> Vec<HorsiesError> {
    profiles
        .iter()
        .filter_map(|(name, profile)| match profile.preload {
            ChildPreload::NoPreload => None,
            ChildPreload::Library | ChildPreload::App => {
                Some(preload_refusal(name, profile.preload))
            }
        })
        .collect()
}

fn preload_refusal(profile_name: &str, preload: ChildPreload) -> HorsiesError {
    HorsiesError::new(format!(
        "worker_profiles.{profile_name}.preload: '{}' is not available to a Rust worker",
        preload.as_str()
    ))
    .with_code(ErrorCode::ConfigInvalidWorkerProfile)
    .with_note(
        "a Rust worker starts each child with exec of its own binary, which holds every \
         task, so there is nothing to preload",
    )
    .with_help(
        "remove preload from this profile: the default is ChildPreload::NoPreload, the only \
         value that a Rust worker takes",
    )
}

#[cfg(test)]
mod tests {
    use horsies_core::manifest::parse_producer_manifest;

    use super::*;

    fn broker(url: &str) -> PostgresConfig {
        PostgresConfig::from_url(url)
    }

    fn manifest_round_trip(manifest: &ProducerManifest) -> ProducerManifest {
        let text = serde_json::to_string(manifest).unwrap();
        parse_producer_manifest(&text).unwrap()
    }

    #[test]
    fn the_core_parser_reads_the_producer_manifest_back() {
        for schema in [
            SchemaMode::MigrateAndValidate,
            SchemaMode::ObserveOnly,
            SchemaMode::AssumeInitialized,
        ] {
            let manifest =
                producer_manifest(&broker("postgresql://app@db.example/horsies"), schema).unwrap();
            assert_eq!(manifest_round_trip(&manifest), manifest);
            assert_eq!(manifest.schema, schema);
        }
    }

    #[test]
    fn the_manifest_holds_the_config_values() {
        let config = PostgresConfig {
            session_database_url: Some("postgresql://app@session.example/horsies".to_owned()),
            pgbouncer_transaction_mode: true,
            pool_size: 7,
            max_overflow: 0,
            pool_timeout: 0,
            pool_recycle: PoolRecycle::Never,
            retain_rerun_input_default: true,
            tcp_keepalives: TcpKeepalivesConfig::Off,
            tcp_user_timeout: TcpUserTimeoutConfig::SystemDefault,
            ..broker("postgresql://app@pool.example/horsies")
        };
        let manifest = producer_manifest(&config, SchemaMode::MigrateAndValidate).unwrap();
        let database = &manifest.database;
        assert_eq!(
            database.database_url.expose(),
            "postgresql://app@pool.example/horsies"
        );
        assert!(matches!(
            &database.session_database,
            SessionDatabase::Separate { url } if url.expose() == "postgresql://app@session.example/horsies"
        ));
        assert!(database.pgbouncer_transaction_mode);
        assert_eq!(database.pool_size.get(), 7);
        assert_eq!(database.max_overflow.get(), 0);
        assert_eq!(database.pool_timeout.get(), 0);
        assert_eq!(database.pool_recycle, PoolRecycle::Never);
        assert_eq!(database.tcp_keepalives, TcpKeepalivesConfig::Off);
        assert_eq!(
            database.tcp_user_timeout,
            TcpUserTimeoutConfig::SystemDefault
        );
        assert!(manifest.send.retain_rerun_input_default);
        assert_eq!(
            manifest.send.idempotency_reservation_window.get(),
            DEFAULT_RESERVATION_WINDOW_MICROS
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
    fn a_pool_size_outside_the_manifest_bounds_is_refused_by_field() {
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
            let refused = producer_manifest(&config, SchemaMode::MigrateAndValidate).unwrap_err();
            assert_eq!(refused.field, field);
            assert_eq!(refused.to_horsies_error().code, None);
        }
    }

    /// An explicit `Library` or `App` is refused with its path; an explicit
    /// `NoPreload` is not.
    #[test]
    fn a_profile_with_a_preload_is_refused_with_its_path() {
        let mut config = AppConfig::for_database_url("postgresql://db/horsies");
        for (name, preload) in [
            ("app", ChildPreload::App),
            ("library", ChildPreload::Library),
            ("plain", ChildPreload::NoPreload),
        ] {
            let profile = WorkerProfile {
                preload,
                ..WorkerProfile::default()
            };
            config.worker_profiles.insert(name.to_owned(), profile);
        }
        let refusals = rust_config_refusals(&config);
        let messages: Vec<&str> = refusals
            .iter()
            .map(|error| error.message.as_str())
            .collect();
        assert_eq!(refusals.len(), 2);
        assert!(messages[0].starts_with("worker_profiles.app.preload"));
        assert!(messages[1].starts_with("worker_profiles.library.preload"));
        assert!(refusals
            .iter()
            .all(|error| error.code == Some(ErrorCode::ConfigInvalidWorkerProfile)));
    }

    #[test]
    fn a_config_without_profiles_has_no_rust_refusal() {
        let config = AppConfig::for_database_url("postgresql://db/horsies");
        assert!(rust_config_refusals(&config).is_empty());
    }
}

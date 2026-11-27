//! The pure checks of the app config and of the declared error codes of a
//! task, which `Horsies.create` calls on a main thread.
//!
//! - Each check starts no core, opens no connection and reads no clock, so a
//!   task child that runs `Horsies.create` before `cli.run` still lets the
//!   child host shape its process runtime.
//! - Each check gives every refusal as a value, in the core's order. A
//!   refusal never throws.
//! - The app config check also gives what the accepted config resolves to:
//!   the send rules of each send (retention, payload bounds and the priority
//!   of each queue), the producer manifest of its broker settings (the
//!   resolution of the Rust client, `config::producer_manifest`) and the
//!   coverage values of its retention (`CoverageRequest::of_retention`).

use horsies_core::core::config::SessionDatabase as CoreSessionDatabase;
use horsies_core::manifest::{
    DatabaseManifest, DatabaseUrl, ManifestVersion, PoolConnections, PoolSize as ManifestPoolSize,
    ProducerManifest, ReservationWindowMicros, SchemaMode, SendManifest, SessionDatabase,
    WholeSeconds,
};
use horsies_core::{
    AppConfig, AppConfigError, CoverageRequest, DeclaredErrorCodes, HorsiesErrorCode,
    PostgresConfig, RetentionConfig,
};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::sends::refusals::{CoreRefusal, RefusalHelp};
use crate::sends::send::{QueuePriorities, SendRules};

/// The reservation window of a keyed send: 24 hours, in microseconds. The
/// window of every app-built broker of the Python client
/// (`IDEMPOTENCY_WINDOW_DEFAULT`) and of the Rust client.
const DEFAULT_RESERVATION_WINDOW_MICROS: u64 = 24 * 60 * 60 * 1_000_000;

/// The URL scheme that the core's connection layer reads.
const CORE_URL_SCHEME: &str = "postgresql://";

/// The short form of [`CORE_URL_SCHEME`] that the config also admits.
const SHORT_URL_SCHEME: &str = "postgres://";

/// The schema mode of the producer manifest of an app: the migrations run
/// at the first operation that needs the schema, as the Python and Rust
/// clients do for an app (`SchemaMode::MigrateAndValidate`).
const APP_SCHEMA_MODE: SchemaMode = SchemaMode::MigrateAndValidate;

/// The values of the coverage pass of a schema install: the history and the
/// heartbeat horizons and the registrable retention periods of the config.
/// `CoreHandle.withInstallCoverage` takes them.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct InstallCoverageValues {
    pub history_horizon_days: f64,
    pub heartbeat_horizon_hours: f64,
    /// The registrable periods, in whole days.
    pub declared_period_days: Vec<f64>,
}

/// The outcome of the check of an app config.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "the value becomes a JavaScript object when the call returns, and napi-rs has no conversion of a Box"
)]
pub enum AppConfigCheck {
    Accepted {
        rules: SendRules,
        manifest_json: String,
        install_coverage: InstallCoverageValues,
    },
    Refused {
        refusals: Vec<CoreRefusal>,
    },
}

/// The outcome of the check of a declared error code list.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeclaredCodesCheck {
    Accepted,
    Refused { refusals: Vec<CoreRefusal> },
}

/// Check the JSON text of the core's `AppConfig`: every rule of
/// `AppConfig::validate`, the manifest bounds of the broker settings and the
/// coverage bounds of the retention. A text that the core's config reader
/// refuses throws `CLIENT_DEFECT` ([`read_app_config`]). Starts nothing.
#[napi(ts_return_type = "NativeEnvelope<AppConfigCheck>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn check_app_config(app_config_json: String) -> Envelope<AppConfigCheck> {
    sync_call("check_app_config", || {
        match read_app_config(&app_config_json) {
            Ok(config) => Envelope::Value(app_config_check(config)),
            Err(defect) => Envelope::Thrown(defect),
        }
    })
}

/// The core's `AppConfig` of a config text, or the `CLIENT_DEFECT` of a text
/// that the core's config reader refuses. The package writes the text from
/// a parsed config, in the serde form of each field, so a refused text is a
/// defect of the package, not a refusal of the user's config. Pure.
fn read_app_config(text: &str) -> Result<AppConfig, NativeThrown> {
    serde_json::from_str(text).map_err(|error| NativeThrown::ClientDefect {
        subject: "checkAppConfig".to_owned(),
        rule: format!("the config text reads as the core's AppConfig: {error}"),
    })
}

/// Check a declared error code list with the core's rule
/// (`DeclaredErrorCodes::parse`): one refusal per refused entry, in the
/// order of the list, with `HRS-112`, `HRS-113` or `HRS-212`. Starts nothing.
#[napi(ts_return_type = "NativeEnvelope<DeclaredCodesCheck>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn parse_declared_error_codes(codes: Vec<String>) -> Envelope<DeclaredCodesCheck> {
    sync_call("parse_declared_error_codes", || {
        Envelope::Value(declared_codes_check(&codes))
    })
}

/// The check of a declared code list. Pure.
fn declared_codes_check(codes: &[String]) -> DeclaredCodesCheck {
    match DeclaredErrorCodes::parse(codes) {
        Ok(_declared) => DeclaredCodesCheck::Accepted,
        Err(refused) => DeclaredCodesCheck::Refused {
            refusals: refused
                .refusals()
                .iter()
                .map(|refusal| {
                    CoreRefusal::coded(
                        refusal.error_code(),
                        refusal.to_string(),
                        Vec::new(),
                        RefusalHelp::NoHelp,
                    )
                })
                .collect(),
        },
    }
}

/// Whether the coverage bounds of the retention can be checked: they hold
/// only for a retention that `RetentionConfig::validate` accepted.
#[derive(Debug, Clone, PartialEq)]
enum CoverageCheck {
    Covered(InstallCoverageValues),
    Refused(CoreRefusal),
    /// The retention has a refusal of its own; its coverage is not checked.
    NotChecked,
}

impl CoverageCheck {
    fn refusal(self) -> Option<CoreRefusal> {
        match self {
            Self::Refused(refusal) => Some(refusal),
            Self::Covered(_) | Self::NotChecked => None,
        }
    }
}

/// The check of an app config. Pure.
///
/// Order of the refusals: the rules of `AppConfig::validate` in its order,
/// then the manifest bounds of the broker settings, then the coverage
/// bounds of the retention.
fn app_config_check(config: AppConfig) -> AppConfigCheck {
    let errors = config.validate();
    let mut refusals: Vec<CoreRefusal> = errors.iter().map(CoreRefusal::of_config_error).collect();
    let manifest = producer_manifest(&config.database);
    let coverage = match errors
        .iter()
        .any(|error| matches!(error, AppConfigError::Retention(_)))
    {
        true => CoverageCheck::NotChecked,
        false => coverage_check(&config.retention),
    };
    // `NotChecked` comes only with a retention refusal, so the last arm
    // always holds at least one refusal.
    match (refusals.is_empty(), manifest, coverage) {
        (true, Ok(manifest), CoverageCheck::Covered(install_coverage)) => {
            AppConfigCheck::Accepted {
                rules: SendRules::new(
                    config.retention,
                    config.payload,
                    QueuePriorities::of(config.queue_mode, config.custom_queues),
                ),
                manifest_json: serde_json::to_string(&manifest).expect(
                    "a producer manifest writes as JSON: every field is a number, a text or a flag",
                ),
                install_coverage,
            }
        }
        (_, manifest, coverage) => {
            refusals.extend(manifest.err().into_iter().flatten());
            refusals.extend(coverage.refusal());
            assert!(
                !refusals.is_empty(),
                "a refused config check holds a refusal: a socket value that the manifest refuses is a refusal of AppConfig::validate"
            );
            AppConfigCheck::Refused { refusals }
        }
    }
}

/// The coverage values of `retention`, or the refusal of its horizons and
/// periods (`HRS-216`). Pure.
fn coverage_check(retention: &RetentionConfig) -> CoverageCheck {
    match CoverageRequest::of_retention(retention) {
        Ok(_request) => CoverageCheck::Covered(InstallCoverageValues {
            history_horizon_days: f64::from(retention.history_leaf_horizon_days),
            heartbeat_horizon_hours: f64::from(retention.heartbeat_leaf_horizon_hours),
            declared_period_days: retention
                .registrable_periods()
                .into_iter()
                .map(|days| f64::from(days.get()))
                .collect(),
        }),
        Err(refused) => CoverageCheck::Refused(CoreRefusal::coded(
            HorsiesErrorCode::ConfigInvalidRetention,
            format!("invalid retention config: {refused}"),
            vec!["the schema install creates the history leaves of these horizons".to_owned()],
            RefusalHelp::Help {
                line: "use horizons and periods inside the bounds of the history leaves".to_owned(),
            },
        )),
    }
}

/// The producer manifest of the broker settings `config`, or every refusal
/// of a pool size outside 1 to 10,000 and of an overflow above 10,000, which
/// the manifest bounds and `PostgresConfig::validate` does not. Each refusal
/// has the code of a broker pool value (`HRS-213`), as Python's pool field
/// checks. Pure.
fn producer_manifest(config: &PostgresConfig) -> Result<ProducerManifest, Vec<CoreRefusal>> {
    Ok(ProducerManifest {
        manifest_version: ManifestVersion::CURRENT,
        database: database_manifest(config)?,
        schema: APP_SCHEMA_MODE,
        send: SendManifest {
            retain_rerun_input_default: config.retain_rerun_input_default,
            idempotency_reservation_window: ReservationWindowMicros::try_from(
                DEFAULT_RESERVATION_WINDOW_MICROS,
            )
            .expect("24 hours is inside the reservation window bounds of 1 us to 30 days"),
        },
    })
}

/// The database section of the manifest, or the refusal of each pool
/// setting outside its manifest bound, in field order. A TCP keepalive or
/// user timeout value outside its bound gives no refusal here:
/// `AppConfig::validate` refuses it (`CONFIG_INVALID_KEEPALIVE`) with the
/// same check, so the refusal list holds it already. Pure.
fn database_manifest(config: &PostgresConfig) -> Result<DatabaseManifest, Vec<CoreRefusal>> {
    let pool_size = ManifestPoolSize::try_from(config.pool_size)
        .map_err(|cause| database_setting_refused("pool_size", &cause));
    let max_overflow = PoolConnections::try_from(config.max_overflow)
        .map_err(|cause| database_setting_refused("max_overflow", &cause));
    let keepalives = config.tcp_keepalives.checked();
    let user_timeout = config.tcp_user_timeout.checked();
    match (pool_size, max_overflow, keepalives, user_timeout) {
        (Ok(pool_size), Ok(max_overflow), Ok(tcp_keepalives), Ok(tcp_user_timeout)) => {
            Ok(DatabaseManifest {
                database_url: core_database_url(&config.database_url),
                session_database: match &config.session_database {
                    CoreSessionDatabase::SameAsDatabaseUrl => SessionDatabase::SameAsDatabaseUrl,
                    CoreSessionDatabase::Separate { url } => SessionDatabase::Separate {
                        url: core_database_url(url),
                    },
                },
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
        (pool_size, max_overflow, _, _) => Err(pool_size
            .err()
            .into_iter()
            .chain(max_overflow.err())
            .collect()),
    }
}

/// The refusal of a database pool setting outside a manifest bound
/// (`HRS-213`). The cause names the bound, never the value. Pure.
fn database_setting_refused(field: &str, cause: &str) -> CoreRefusal {
    CoreRefusal::coded(
        HorsiesErrorCode::ConfigInvalidDatabasePool,
        format!("invalid database config: database.{field}: {cause}"),
        Vec::new(),
        RefusalHelp::NoHelp,
    )
}

/// `url` with the scheme that the core reads: `postgres://` becomes
/// `postgresql://`, the rest is kept (Python `core_database_url`).
/// `PostgresConfig::validate` admits only these two schemes. Pure.
fn core_database_url(url: &str) -> DatabaseUrl {
    let text = match url.strip_prefix(SHORT_URL_SCHEME) {
        Some(rest) => format!("{CORE_URL_SCHEME}{rest}"),
        None => url.to_owned(),
    };
    // A `DatabaseUrl` is built only from its serde form, one JSON string, so
    // that no code path holds an unchecked URL type. A string always reads.
    serde_json::from_value(serde_json::Value::String(text))
        .expect("a JSON string reads as a DatabaseUrl")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The JSON text of an app config with `broker` as its broker section.
    fn config_text(broker: &str) -> String {
        format!(r#"{{"database": {broker}}}"#)
    }

    const BROKER: &str = r#"{"database_url": "postgres://user:secret@127.0.0.1:55432/horsies"}"#;

    /// The check of a config text that the core's config reader reads.
    fn check_of(text: &str) -> AppConfigCheck {
        app_config_check(read_app_config(text).expect("the config text reads"))
    }

    fn refusals_of(check: AppConfigCheck) -> Vec<CoreRefusal> {
        match check {
            AppConfigCheck::Refused { refusals } => refusals,
            AppConfigCheck::Accepted { .. } => panic!("the config is refused"),
        }
    }

    #[test]
    fn a_config_with_only_a_database_url_is_accepted_with_the_core_defaults() {
        let AppConfigCheck::Accepted {
            manifest_json,
            install_coverage,
            ..
        } = check_of(&config_text(BROKER))
        else {
            panic!("a config with a database URL is accepted");
        };
        let manifest = horsies_core::manifest::parse_producer_manifest(&manifest_json)
            .expect("the manifest reads back");
        assert_eq!(
            manifest.database.database_url.expose(),
            "postgresql://user:secret@127.0.0.1:55432/horsies"
        );
        assert_eq!(manifest.schema, SchemaMode::MigrateAndValidate);
        let defaults = RetentionConfig::default();
        assert_eq!(
            install_coverage.history_horizon_days,
            f64::from(defaults.history_leaf_horizon_days)
        );
    }

    #[test]
    fn a_text_that_the_config_reader_refuses_is_a_client_defect() {
        let Err(NativeThrown::ClientDefect { subject, rule }) =
            read_app_config(&format!(r#"{{"database": {BROKER}, "unknown_key": 1}}"#))
        else {
            panic!("a text with an unknown key is a client defect");
        };
        assert_eq!(subject, "checkAppConfig");
        assert!(
            rule.starts_with("the config text reads as the core's AppConfig: "),
            "{rule}"
        );
    }

    #[test]
    fn a_rule_of_the_core_validation_is_refused_with_its_catalog_code() {
        let text = format!(r#"{{"database": {BROKER}, "queue_mode": "custom"}}"#);
        let refusals = refusals_of(check_of(&text));
        assert_eq!(refusals[0].code, "HRS-200");
    }

    #[test]
    fn a_payload_rule_is_refused_with_hrs_227() {
        let text = format!(r#"{{"database": {BROKER}, "payload": {{"warn_bytes": 0}}}}"#);
        let refusals = refusals_of(check_of(&text));
        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert_eq!(refusals[0].code, "HRS-227");
        assert!(
            refusals[0].message.contains("payload.warn_bytes"),
            "{}",
            refusals[0].message
        );
    }

    #[test]
    fn a_pool_size_or_overflow_above_the_manifest_bound_is_refused_with_hrs_213() {
        for (setting, field) in [
            (r#""pool_size": 10001"#, "database.pool_size"),
            (r#""max_overflow": 10001"#, "database.max_overflow"),
        ] {
            let broker =
                format!(r#"{{"database_url": "postgresql://127.0.0.1/horsies", {setting}}}"#);
            let refusals = refusals_of(check_of(&config_text(&broker)));
            let pool = refusals
                .iter()
                .find(|refusal| refusal.message.contains(field))
                .unwrap_or_else(|| panic!("{field} is refused: {refusals:?}"));
            assert_eq!(pool.code, "HRS-213");
            assert_eq!(pool.help, RefusalHelp::NoHelp);
        }
    }

    #[test]
    fn a_pool_size_and_an_overflow_above_the_bounds_are_two_refusals() {
        let broker = r#"{"database_url": "postgresql://127.0.0.1/horsies", "pool_size": 10001, "max_overflow": 10001}"#;
        let refusals = refusals_of(check_of(&config_text(broker)));
        let pool: Vec<&str> = refusals
            .iter()
            .filter(|refusal| refusal.code == "HRS-213")
            .map(|refusal| refusal.message.as_str())
            .collect();
        assert_eq!(pool.len(), 2, "{refusals:?}");
        assert!(pool[0].contains("database.pool_size"), "{}", pool[0]);
        assert!(pool[1].contains("database.max_overflow"), "{}", pool[1]);
    }

    #[test]
    fn a_horizon_that_the_coverage_bounds_refuse_is_hrs_216_with_its_help() {
        let retention = RetentionConfig {
            history_leaf_horizon_days: 1,
            ..RetentionConfig::default()
        };
        let CoverageCheck::Refused(refusal) = coverage_check(&retention) else {
            panic!("a horizon of one day is below the coverage bound");
        };
        assert_eq!(refusal.code, "HRS-216");
        assert!(
            refusal.message.starts_with("invalid retention config: "),
            "{}",
            refusal.message
        );
        assert!(matches!(refusal.help, RefusalHelp::Help { .. }));
    }

    /// The same horizon in a config: the retention rule refuses it, and the
    /// coverage is not checked, so the config has one refusal, not two.
    #[test]
    fn a_retention_refusal_leaves_the_coverage_not_checked() {
        let text =
            format!(r#"{{"database": {BROKER}, "retention": {{"history_leaf_horizon_days": 1}}}}"#);
        let refusals = refusals_of(check_of(&text));
        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert_eq!(refusals[0].code, "HRS-216");
    }

    #[test]
    fn a_declared_code_list_with_a_built_in_name_is_refused_with_hrs_212() {
        let check = declared_codes_check(&["TASK_TIMEOUT".to_owned()]);
        let DeclaredCodesCheck::Refused { refusals } = check else {
            panic!("a built-in name is refused");
        };
        assert_eq!(refusals[0].code, "HRS-212");
        assert_eq!(refusals[0].help, RefusalHelp::NoHelp);
    }

    #[test]
    fn a_repeated_declared_code_is_refused_with_hrs_113_and_a_bad_text_with_hrs_112() {
        let check = declared_codes_check(&[
            "CARD_DECLINED".to_owned(),
            "CARD_DECLINED".to_owned(),
            "lower_case".to_owned(),
        ]);
        let DeclaredCodesCheck::Refused { refusals } = check else {
            panic!("a repeat and a bad text are refused");
        };
        let codes: Vec<String> = refusals.into_iter().map(|refusal| refusal.code).collect();
        assert_eq!(codes, vec!["HRS-113".to_owned(), "HRS-112".to_owned()]);
    }

    #[test]
    fn a_declared_code_list_that_holds_each_rule_is_accepted() {
        assert_eq!(
            declared_codes_check(&["CARD_DECLINED".to_owned(), "PROVIDER_DOWN".to_owned()]),
            DeclaredCodesCheck::Accepted
        );
        assert_eq!(declared_codes_check(&[]), DeclaredCodesCheck::Accepted);
    }
}

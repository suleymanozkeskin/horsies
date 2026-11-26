//! The pure checks of the app config and of the declared error codes of a
//! task, which `Horsies.create` calls on a main thread.
//!
//! - Each check starts no core, opens no connection and reads no clock, so a
//!   task child that runs `Horsies.create` before `cli.run` still lets the
//!   child host shape its process runtime.
//! - Each check gives every refusal as a value, in the core's order. A
//!   refusal never throws.
//! - The app config check also gives what the accepted config resolves to:
//!   the send rules of each send, the producer manifest of its broker
//!   settings (the resolution of the Rust client, `config::producer_manifest`)
//!   and the coverage values of its retention (`CoverageRequest::of_retention`).

use horsies_core::manifest::{
    DatabaseManifest, DatabaseUrl, ManifestVersion, PoolConnections, PoolSize as ManifestPoolSize,
    ProducerManifest, ReservationWindowMicros, SchemaMode, SendManifest, SessionDatabase,
    WholeSeconds,
};
use horsies_core::{
    AppConfig, AppConfigError, CoverageRequest, DeclaredErrorCodes, ErrorCode, PostgresConfig,
    RetentionConfig,
};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;
use crate::sends::refusals::CoreRefusal;
use crate::sends::send::SendRules;

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

/// Check the JSON text of the core's `AppConfig`: its reader, every rule of
/// `AppConfig::validate`, the manifest bounds of the broker settings and the
/// coverage bounds of the retention. Starts nothing.
#[napi(ts_return_type = "NativeEnvelope<AppConfigCheck>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn check_app_config(app_config_json: String) -> Envelope<AppConfigCheck> {
    sync_call("check_app_config", || {
        Envelope::Value(app_config_check(&app_config_json))
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
                        Vec::new(),
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

/// The check of an app config text. Pure.
///
/// Order of the refusals: the rules of `AppConfig::validate` in its order,
/// then the manifest bounds of the broker settings, then the coverage
/// bounds of the retention.
fn app_config_check(text: &str) -> AppConfigCheck {
    let config: AppConfig = match serde_json::from_str(text) {
        Ok(config) => config,
        Err(error) => {
            return AppConfigCheck::Refused {
                refusals: vec![CoreRefusal::uncoded(format!(
                    "the app config does not read: {error}"
                ))],
            }
        }
    };
    let errors = config.validate();
    let mut refusals: Vec<CoreRefusal> = errors
        .iter()
        .map(|error| CoreRefusal::of_error(&error.to_horsies_error()))
        .collect();
    let manifest = producer_manifest(&config.broker);
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
                rules: SendRules::new(config.retention, config.payload),
                manifest_json: serde_json::to_string(&manifest).expect(
                    "a producer manifest writes as JSON: every field is a number, a text or a flag",
                ),
                install_coverage,
            }
        }
        (_, manifest, coverage) => {
            refusals.extend(manifest.err());
            refusals.extend(coverage.refusal());
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
            ErrorCode::ConfigInvalidRetention,
            format!("invalid retention config: {refused}"),
            vec!["the schema install creates the history leaves of these horizons".to_owned()],
            vec!["use horizons and periods inside the bounds of the history leaves".to_owned()],
        )),
    }
}

/// The producer manifest of the broker settings `config`, or the refusal of
/// a pool size outside 1 to 10,000 or an overflow above 10,000, which the
/// manifest bounds and `PostgresConfig::validate` does not. No catalog code:
/// Python refuses these values through field bounds. Pure.
fn producer_manifest(config: &PostgresConfig) -> Result<ProducerManifest, CoreRefusal> {
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

fn database_manifest(config: &PostgresConfig) -> Result<DatabaseManifest, CoreRefusal> {
    let pool_size = ManifestPoolSize::try_from(config.pool_size)
        .map_err(|cause| broker_setting_refused("pool_size", &cause))?;
    let max_overflow = PoolConnections::try_from(config.max_overflow)
        .map_err(|cause| broker_setting_refused("max_overflow", &cause))?;
    Ok(DatabaseManifest {
        database_url: core_database_url(&config.database_url),
        session_database: match &config.session_database_url {
            None => SessionDatabase::SameAsDatabaseUrl,
            Some(url) => SessionDatabase::Separate {
                url: core_database_url(url),
            },
        },
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

/// The refusal of a broker setting outside a manifest bound. The cause
/// names the bound, never the value. Pure.
fn broker_setting_refused(field: &str, cause: &str) -> CoreRefusal {
    CoreRefusal::uncoded(format!("invalid broker config: broker.{field}: {cause}"))
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
    use crate::sends::refusals::CoreRefusalCode;

    /// The JSON text of an app config with `broker` as its broker section.
    fn config_text(broker: &str) -> String {
        format!(r#"{{"broker": {broker}}}"#)
    }

    const BROKER: &str = r#"{"database_url": "postgres://user:secret@127.0.0.1:55432/horsies"}"#;

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
        } = app_config_check(&config_text(BROKER))
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
    fn a_text_that_the_config_reader_refuses_is_one_refusal_with_no_code() {
        let refusals = refusals_of(app_config_check(r#"{"broker": {}, "unknown_key": 1}"#));
        assert_eq!(refusals.len(), 1);
        assert_eq!(refusals[0].code, CoreRefusalCode::NoCode);
        assert!(
            refusals[0]
                .message
                .starts_with("the app config does not read: "),
            "{}",
            refusals[0].message
        );
    }

    #[test]
    fn a_rule_of_the_core_validation_is_refused_with_its_catalog_code() {
        let text = format!(r#"{{"broker": {BROKER}, "queue_mode": "custom"}}"#);
        let refusals = refusals_of(app_config_check(&text));
        assert_eq!(
            refusals[0].code,
            CoreRefusalCode::Catalog {
                code: "HRS-200".to_owned()
            }
        );
    }

    #[test]
    fn a_pool_size_above_the_manifest_bound_is_refused_with_no_code() {
        let broker = r#"{"database_url": "postgresql://127.0.0.1/horsies", "pool_size": 10001}"#;
        let refusals = refusals_of(app_config_check(&config_text(broker)));
        let pool = refusals
            .iter()
            .find(|refusal| refusal.message.contains("broker.pool_size"))
            .expect("the pool size is refused");
        assert_eq!(pool.code, CoreRefusalCode::NoCode);
    }

    #[test]
    fn a_declared_code_list_with_a_built_in_name_is_refused_with_hrs_212() {
        let check = declared_codes_check(&["TASK_TIMEOUT".to_owned()]);
        let DeclaredCodesCheck::Refused { refusals } = check else {
            panic!("a built-in name is refused");
        };
        assert_eq!(
            refusals[0].code,
            CoreRefusalCode::Catalog {
                code: "HRS-212".to_owned()
            }
        );
        assert!(refusals[0].help.is_empty());
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
        let codes: Vec<CoreRefusalCode> =
            refusals.into_iter().map(|refusal| refusal.code).collect();
        assert_eq!(
            codes,
            vec![
                CoreRefusalCode::Catalog {
                    code: "HRS-113".to_owned()
                },
                CoreRefusalCode::Catalog {
                    code: "HRS-112".to_owned()
                },
            ]
        );
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

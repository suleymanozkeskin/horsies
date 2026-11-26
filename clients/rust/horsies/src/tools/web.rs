//! `horsies web`: serve the monitoring web UI of one database (Python
//! `horsies web`) through the axum mount of the core's monitoring handler.
//!
//! The binary has no task of an app, so it serves a database: from a TOML
//! file of `AppConfig` or from `--database-url`. The handle never installs or
//! migrates a schema (`SchemaMode::ObserveOnly`). An app that needs its
//! workflow definitions for the resume of a paused workflow mounts
//! `horsies::web::router` in its own binary.
//!
//! Without the `web` feature the command exits 1 and says so.

// Without the `web` feature only the refusal runs; the rest stays tested.
#![cfg_attr(not(feature = "web"), allow(dead_code))]

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, ValueEnum};

use crate::cli::LogLevel;
use crate::{AppConfig, HorsiesError, SessionDatabase};

/// The largest TOML config file that `web` reads.
const MAX_CONFIG_BYTES: u64 = 1024 * 1024;

/// The warning of `--auth trusted-header` (Python `_TRUSTED_HEADER_WARNING`).
const TRUSTED_HEADER_WARNING: &str = "SECURITY: the reverse proxy in front of this server MUST \
strip or overwrite the {header} header on incoming requests. A proxy that forwards a \
client-supplied header makes this mode trivially spoofable, and horsies cannot detect that.";

/// The arguments of `horsies web`.
#[derive(Debug, Clone, Args)]
pub(super) struct WebArgs {
    /// A TOML file of AppConfig; its broker section names the database. Omit
    /// it when using --database-url.
    #[arg(
        value_name = "CONFIG",
        conflicts_with = "database_url",
        required_unless_present = "database_url"
    )]
    config: Option<PathBuf>,
    /// Monitor this database, with the default value of every other setting.
    #[arg(long, value_name = "URL")]
    database_url: Option<String>,
    /// Direct or session-capable URL when --database-url is pooled.
    #[arg(long, value_name = "URL", requires = "database_url")]
    session_database_url: Option<String>,
    /// --database-url is a PgBouncer transaction-pool endpoint.
    #[arg(long, requires_all = ["database_url", "session_database_url"])]
    pgbouncer_transaction_mode: bool,
    /// Bind address.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// Bind port; 0 asks the operating system for a free port.
    #[arg(long, default_value_t = 8600)]
    port: u16,
    /// Authorization mode: none (a loopback host only) or trusted-header.
    #[arg(long, value_enum, default_value_t = AuthMode::None)]
    auth: AuthMode,
    /// Identity header that the proxy sets, in trusted-header mode.
    #[arg(long, default_value = "X-Forwarded-User")]
    trusted_header: String,
    /// Allow cancel, pause and resume (default: view only).
    #[arg(long)]
    enable_actions: bool,
    /// A stylesheet URL that the page loads after its own stylesheet.
    #[arg(long, value_name = "URL")]
    custom_css_url: Option<String>,
    /// The link of a task's trace id in the task detail: an http or https
    /// URL with one {trace_id}, for example
    /// https://grafana.example.com/explore?traceId={trace_id}.
    #[arg(long, value_name = "TEMPLATE")]
    trace_link: Option<String>,
    #[arg(long, value_enum, ignore_case = true, default_value = "INFO")]
    loglevel: LogLevel,
}

/// The `--auth` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum AuthMode {
    None,
    TrustedHeader,
}

/// Where the config of the served database comes from.
enum ConfigSource<'a> {
    TomlFile(&'a Path),
    DatabaseUrl {
        url: &'a str,
        session_url: Option<&'a str>,
        pgbouncer_transaction_mode: bool,
    },
}

/// The access policy of the served UI (Python `resolve_web_auth_policy`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum WebPolicy {
    /// `--auth none --enable-actions` on a loopback host.
    AllowAll,
    /// `--auth none` on a loopback host.
    ViewOnly,
    /// `--auth trusted-header`: the header that the proxy sets.
    TrustedHeader { header: String },
}

/// Why `web` did not serve, or stopped serving with a failure.
#[derive(Debug, thiserror::Error)]
enum WebError {
    #[cfg(not(feature = "web"))]
    #[error("horsies web requires the `web` cargo feature")]
    MissingFeature,
    #[error(
        "--auth none is only allowed on a loopback host; {host:?} is reachable from the \
         network. Use --auth trusted-header with a proxy-set header."
    )]
    ExposedWithoutAuthentication { host: String },
    #[error("--trusted-header requires a header name")]
    BlankTrustedHeader,
    #[error("invalid trusted header name: {cause}")]
    InvalidTrustedHeader { cause: String },
    #[error("invalid --trace-link: {cause}")]
    InvalidTraceLink { cause: String },
    #[error("could not read configuration file {path}: {cause}")]
    ConfigRead {
        path: PathBuf,
        cause: std::io::Error,
    },
    #[error("configuration file {path} is larger than {limit} bytes")]
    ConfigTooLarge { path: PathBuf, limit: u64 },
    #[error("could not parse TOML configuration file {path}: {cause}")]
    ConfigParse {
        path: PathBuf,
        cause: toml::de::Error,
    },
    #[error("invalid application configuration: {0}")]
    ConfigInvalid(HorsiesError),
    #[error("the core runtime is not available: {0}")]
    RuntimeUnavailable(String),
    #[error("could not bind the monitoring server to {host}:{port}: {cause}")]
    Bind {
        host: String,
        port: u16,
        cause: std::io::Error,
    },
    #[error("the monitoring server failed: {0}")]
    Serve(std::io::Error),
}

impl WebError {
    /// 2 for an argument that `web` refuses (Python `_web_arg_error`), 1 for
    /// every other failure.
    const fn exit_code(&self) -> u8 {
        match self {
            Self::ExposedWithoutAuthentication { .. }
            | Self::BlankTrustedHeader
            | Self::InvalidTrustedHeader { .. }
            | Self::InvalidTraceLink { .. } => 2,
            #[cfg(not(feature = "web"))]
            Self::MissingFeature => 1,
            Self::ConfigRead { .. }
            | Self::ConfigTooLarge { .. }
            | Self::ConfigParse { .. }
            | Self::ConfigInvalid(_)
            | Self::RuntimeUnavailable(_)
            | Self::Bind { .. }
            | Self::Serve(_) => 1,
        }
    }
}

/// Run `web` and return its exit status. An argument error is printed as
/// `horsies web: error: ...`, as argparse prints one; another failure as
/// its text.
pub(super) fn run(args: &WebArgs) -> ExitCode {
    match serve(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let code = error.exit_code();
            match code {
                2 => eprintln!("horsies web: error: {error}"),
                _ => eprintln!("{error}"),
            }
            ExitCode::from(code)
        }
    }
}

#[cfg(not(feature = "web"))]
fn serve(_args: &WebArgs) -> Result<(), WebError> {
    Err(WebError::MissingFeature)
}

/// Check the arguments, build the app of the served database, and serve its
/// UI until `SIGTERM` or `SIGINT`.
#[cfg(feature = "web")]
fn serve(args: &WebArgs) -> Result<(), WebError> {
    use crate::process_state::runtime_for;
    use crate::web::{Actions, MonitoringConfig, MonitoringUiConfig};
    use crate::{Horsies, SchemaMode};

    let trace_link = trace_link_of(args.trace_link.as_deref())?;
    let policy = web_policy(
        args.auth,
        &args.host,
        &args.trusted_header,
        args.enable_actions,
    )?;
    let built = built_policy(policy, args.enable_actions)?;
    if let BuiltPolicy::TrustedHeader(_) = &built {
        eprintln!(
            "{}",
            TRUSTED_HEADER_WARNING.replace("{header}", &args.trusted_header)
        );
    }
    crate::cli::logging::install_logs(args.loglevel.child_level(), crate::cli::LogFormat::Text);
    let config = resolve_config(&config_source(args))?;
    let app = Horsies::with_schema_mode(config, SchemaMode::ObserveOnly)
        .map_err(WebError::ConfigInvalid)?;
    let monitoring = MonitoringConfig {
        ui: MonitoringUiConfig {
            custom_css_url: args.custom_css_url.clone(),
            trace_link,
        },
        actions: match args.enable_actions {
            true => Actions::Enabled,
            false => Actions::Disabled,
        },
    };
    let router = match built {
        BuiltPolicy::AllowAll => crate::web::router(&app, monitoring, crate::web::AllowAll),
        BuiltPolicy::ViewOnly => crate::web::router(&app, monitoring, crate::web::ViewOnly),
        BuiltPolicy::TrustedHeader(policy) => crate::web::router(&app, monitoring, policy),
    };
    let runtime = runtime_for(format_args!("horsies web"))
        .map_err(|failed| WebError::RuntimeUnavailable(failed.to_string()))?;
    runtime
        .runtime()
        .block_on(serve_router(router, &args.host, args.port))
}

/// The trace link of `--trace-link`: `NoTraceLink` without the flag, else
/// the checked template. Errors: `InvalidTraceLink` with the rule that the
/// text breaks (an argument error). Pure.
#[cfg(feature = "web")]
fn trace_link_of(text: Option<&str>) -> Result<crate::web::TraceLink, WebError> {
    use crate::web::{TraceLink, TraceLinkTemplate};

    match text {
        None => Ok(TraceLink::NoTraceLink),
        Some(text) => TraceLinkTemplate::parse(text)
            .map(TraceLink::Template)
            .map_err(|refused| WebError::InvalidTraceLink {
                cause: refused.to_string(),
            }),
    }
}

/// The policy of the axum mount.
#[cfg(feature = "web")]
enum BuiltPolicy {
    AllowAll,
    ViewOnly,
    TrustedHeader(crate::web::TrustedHeader),
}

/// The mount's policy of `policy`. A trusted-header policy of a value that
/// is not a header name is an argument error.
///
/// Errors: [`WebError::InvalidTrustedHeader`].
#[cfg(feature = "web")]
fn built_policy(policy: WebPolicy, enable_actions: bool) -> Result<BuiltPolicy, WebError> {
    match policy {
        WebPolicy::AllowAll => Ok(BuiltPolicy::AllowAll),
        WebPolicy::ViewOnly => Ok(BuiltPolicy::ViewOnly),
        WebPolicy::TrustedHeader { header } => {
            crate::web::TrustedHeader::new(&header, enable_actions)
                .map(BuiltPolicy::TrustedHeader)
                .map_err(|cause| WebError::InvalidTrustedHeader {
                    cause: cause.to_string(),
                })
        }
    }
}

/// Bind `host:port` and serve `router` until `SIGTERM` or `SIGINT`; requests
/// in flight then end before the return.
#[cfg(feature = "web")]
async fn serve_router(router: axum::Router, host: &str, port: u16) -> Result<(), WebError> {
    let listener = tokio::net::TcpListener::bind((host, port))
        .await
        .map_err(|cause| WebError::Bind {
            host: host.to_owned(),
            port,
            cause,
        })?;
    let address = listener.local_addr().map_err(WebError::Serve)?;
    tracing::info!("Serving horsies monitoring UI on http://{address}");
    axum::serve(listener, router)
        .with_graceful_shutdown(crate::signals::stop_on_signals("monitoring server", || {}))
        .await
        .map_err(WebError::Serve)
}

/// The policy of the arguments (Python `resolve_web_auth_policy`). A host
/// that is not a loopback host refuses `--auth none`: anyone who can reach
/// the port could read the task data. Pure.
///
/// Errors: [`WebError::ExposedWithoutAuthentication`],
/// [`WebError::BlankTrustedHeader`].
fn web_policy(
    auth: AuthMode,
    host: &str,
    trusted_header: &str,
    enable_actions: bool,
) -> Result<WebPolicy, WebError> {
    match (auth, enable_actions) {
        (AuthMode::TrustedHeader, _) => match trusted_header.trim().is_empty() {
            true => Err(WebError::BlankTrustedHeader),
            false => Ok(WebPolicy::TrustedHeader {
                header: trusted_header.to_owned(),
            }),
        },
        (AuthMode::None, _) if !is_loopback_host(host) => {
            Err(WebError::ExposedWithoutAuthentication {
                host: host.to_owned(),
            })
        }
        (AuthMode::None, true) => Ok(WebPolicy::AllowAll),
        (AuthMode::None, false) => Ok(WebPolicy::ViewOnly),
    }
}

/// Whether a bind address reaches only this machine (Python `_is_loopback`).
/// A host that does not parse as an address is taken as exposed. Pure.
fn is_loopback_host(host: &str) -> bool {
    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

/// The source that the arguments name: clap admits exactly one of `CONFIG`
/// and `--database-url`. Pure.
fn config_source(args: &WebArgs) -> ConfigSource<'_> {
    match (&args.config, &args.database_url) {
        (Some(path), None) => ConfigSource::TomlFile(path),
        (None, Some(url)) => ConfigSource::DatabaseUrl {
            url,
            session_url: args.session_database_url.as_deref(),
            pgbouncer_transaction_mode: args.pgbouncer_transaction_mode,
        },
        (Some(_), Some(_)) | (None, None) => {
            unreachable!("clap admits exactly one of CONFIG and --database-url")
        }
    }
}

/// The config of `source`. A TOML file is read up to [`MAX_CONFIG_BYTES`]
/// and parsed as `AppConfig`, whose sections refuse an unknown key by name.
/// `--database-url` gives the default value of every other setting.
///
/// Errors: [`WebError::ConfigRead`], [`WebError::ConfigTooLarge`],
/// [`WebError::ConfigParse`]. The config is validated when the app is built.
fn resolve_config(source: &ConfigSource<'_>) -> Result<AppConfig, WebError> {
    match source {
        ConfigSource::TomlFile(path) => {
            let text = read_config_file(path)?;
            parse_config(path, &text)
        }
        ConfigSource::DatabaseUrl {
            url,
            session_url,
            pgbouncer_transaction_mode,
        } => {
            let mut config = AppConfig::for_database_url(*url);
            config.broker.session_database = match session_url {
                None => SessionDatabase::SameAsDatabaseUrl,
                Some(url) => SessionDatabase::Separate {
                    url: (*url).to_owned(),
                },
            };
            config.broker.pgbouncer_transaction_mode = *pgbouncer_transaction_mode;
            Ok(config)
        }
    }
}

/// The text of the file at `path`, at most [`MAX_CONFIG_BYTES`] long.
///
/// Errors: [`WebError::ConfigRead`] (the file cannot be opened or read, or
/// it is not UTF-8), [`WebError::ConfigTooLarge`].
fn read_config_file(path: &Path) -> Result<String, WebError> {
    let read_error = |cause| WebError::ConfigRead {
        path: path.to_path_buf(),
        cause,
    };
    let file = std::fs::File::open(path).map_err(read_error)?;
    let mut text = String::new();
    let _read = file
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(read_error)?;
    match u64::try_from(text.len()) {
        Ok(length) if length <= MAX_CONFIG_BYTES => Ok(text),
        Ok(_) | Err(_) => Err(WebError::ConfigTooLarge {
            path: path.to_path_buf(),
            limit: MAX_CONFIG_BYTES,
        }),
    }
}

/// `text` parsed as `AppConfig`. Pure.
///
/// Errors: [`WebError::ConfigParse`], which names the key or the value that
/// did not parse.
fn parse_config(path: &Path, text: &str) -> Result<AppConfig, WebError> {
    toml::from_str::<AppConfig>(text).map_err(|cause| WebError::ConfigParse {
        path: path.to_path_buf(),
        cause,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_loopback_rule_is_exact_and_fails_closed() {
        for host in ["localhost", "127.0.0.1", "::1", "127.0.0.2"] {
            assert!(is_loopback_host(host), "{host}");
        }
        for host in [
            "0.0.0.0",
            "192.168.1.4",
            "example.com",
            "localhost.local",
            "",
        ] {
            assert!(!is_loopback_host(host), "{host}");
        }
    }

    #[test]
    fn auth_none_gives_allow_all_or_view_only_on_a_loopback_host() {
        let allow = web_policy(AuthMode::None, "127.0.0.1", "X-User", true).unwrap();
        assert_eq!(allow, WebPolicy::AllowAll);
        let view = web_policy(AuthMode::None, "localhost", "X-User", false).unwrap();
        assert_eq!(view, WebPolicy::ViewOnly);
    }

    #[test]
    fn auth_none_on_an_exposed_host_is_an_argument_error() {
        let refused = web_policy(AuthMode::None, "0.0.0.0", "X-User", false).unwrap_err();
        assert!(matches!(
            &refused,
            WebError::ExposedWithoutAuthentication { host } if host == "0.0.0.0"
        ));
        assert_eq!(refused.exit_code(), 2);
    }

    #[test]
    fn a_trusted_header_is_taken_on_any_host_and_a_blank_one_is_refused() {
        let trusted = web_policy(AuthMode::TrustedHeader, "0.0.0.0", "X-User", true).unwrap();
        assert_eq!(
            trusted,
            WebPolicy::TrustedHeader {
                header: "X-User".to_owned()
            }
        );
        let blank = web_policy(AuthMode::TrustedHeader, "0.0.0.0", "  ", true).unwrap_err();
        assert!(matches!(blank, WebError::BlankTrustedHeader));
        assert_eq!(blank.exit_code(), 2);
    }

    #[test]
    fn a_toml_config_keeps_its_broker_section() {
        let config = parse_config(
            Path::new("horsies.toml"),
            "[broker]\ndatabase_url = \"postgresql://pool/horsies\"\npgbouncer_transaction_mode = \
             true\n[broker.session_database]\nkind = \"SEPARATE\"\nurl = \
             \"postgresql://direct/horsies\"\n",
        )
        .unwrap();
        assert_eq!(config.broker.database_url, "postgresql://pool/horsies");
        assert_eq!(
            config.broker.effective_session_database_url(),
            "postgresql://direct/horsies"
        );
        assert!(config.broker.pgbouncer_transaction_mode);
    }

    /// #553: a key that no config section has is refused by name.
    #[test]
    fn an_unknown_toml_key_is_refused_by_name() {
        for (text, key) in [
            (
                "databse_url = \"x\"\n[broker]\ndatabase_url = \"postgresql://h/d\"\n",
                "databse_url",
            ),
            (
                "[broker]\ndatabase_url = \"postgresql://h/d\"\npool_sise = 3\n",
                "pool_sise",
            ),
        ] {
            let refused = parse_config(Path::new("horsies.toml"), text).unwrap_err();
            assert!(matches!(refused, WebError::ConfigParse { .. }));
            assert!(refused.to_string().contains(key), "{refused}");
            assert_eq!(refused.exit_code(), 1);
        }
    }

    #[test]
    fn database_url_mode_sets_the_session_url_and_the_pooler_mode() {
        let config = resolve_config(&ConfigSource::DatabaseUrl {
            url: "postgresql://pool/horsies",
            session_url: Some("postgresql://direct/horsies"),
            pgbouncer_transaction_mode: true,
        })
        .unwrap();
        assert_eq!(config.broker.database_url, "postgresql://pool/horsies");
        assert_eq!(
            config.broker.effective_session_database_url(),
            "postgresql://direct/horsies"
        );
        assert!(config.broker.pgbouncer_transaction_mode);
    }

    #[test]
    fn a_config_file_above_the_bound_is_refused() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let line = "# padding line of the bound test\n";
        let lines = usize::try_from(MAX_CONFIG_BYTES).unwrap() / line.len() + 1;
        std::io::Write::write_all(&mut file, line.repeat(lines).as_bytes()).unwrap();
        let refused = read_config_file(file.path()).unwrap_err();
        assert!(matches!(refused, WebError::ConfigTooLarge { .. }));
        let missing = read_config_file(Path::new("/nonexistent/horsies.toml")).unwrap_err();
        assert!(matches!(missing, WebError::ConfigRead { .. }));
    }

    #[cfg(feature = "web")]
    #[test]
    fn a_trace_link_is_absent_without_the_flag_and_a_bad_template_is_an_argument_error() {
        use crate::web::TraceLink;

        assert!(matches!(trace_link_of(None), Ok(TraceLink::NoTraceLink)));
        let template = "https://grafana.example.com/explore?traceId={trace_id}";
        match trace_link_of(Some(template)) {
            Ok(TraceLink::Template(checked)) => assert_eq!(checked.as_str(), template),
            other => panic!("the template is valid: {other:?}"),
        }
        let refused = trace_link_of(Some("ftp://example.com/{trace_id}"))
            .expect_err("another scheme is refused");
        assert!(
            matches!(refused, WebError::InvalidTraceLink { .. }),
            "{refused:?}"
        );
        assert_eq!(refused.exit_code(), 2);
    }
}

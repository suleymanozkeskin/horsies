//! Monitoring web-server CLI.

#[cfg(any(feature = "web", test))]
use std::net::IpAddr;
#[cfg(feature = "web")]
use std::path::Path;
use std::path::PathBuf;

use clap::{Args, ValueEnum};

use super::LogLevel;
#[cfg(feature = "web")]
use crate::core::config::AppConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum WebAuthMode {
    None,
    TrustedHeader,
}

#[derive(Debug, Clone, Args)]
pub struct WebArgs {
    /// TOML application configuration file.
    #[arg(
        value_name = "CONFIG",
        conflicts_with = "database_url",
        required_unless_present = "database_url"
    )]
    pub config: Option<PathBuf>,

    /// PostgreSQL runtime URL for read-only database mode.
    #[arg(long, value_name = "URL", conflicts_with = "config")]
    pub database_url: Option<String>,

    /// Direct or session-pooled URL for LISTEN/NOTIFY.
    #[arg(long, value_name = "URL", requires = "database_url")]
    pub session_database_url: Option<String>,

    /// Treat database-url as a PgBouncer transaction-pool endpoint.
    #[arg(long, requires_all = ["database_url", "session_database_url"])]
    pub pgbouncer_transaction_mode: bool,

    /// Interface or host to bind.
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// TCP port to bind. Zero requests an operating-system assigned port.
    #[arg(long, default_value_t = 8600)]
    pub port: u16,

    /// Authentication policy.
    #[arg(long, value_enum, default_value_t = WebAuthMode::None)]
    pub auth: WebAuthMode,

    /// Identity header trusted in trusted-header mode.
    #[arg(long, default_value = "X-Forwarded-User")]
    pub trusted_header: String,

    /// Enable task and workflow actions.
    #[arg(long)]
    pub enable_actions: bool,

    /// Optional stylesheet URL injected after the bundled stylesheet.
    #[arg(long)]
    pub custom_css_url: Option<String>,

    /// Logging level.
    #[arg(long, default_value = "info")]
    pub loglevel: LogLevel,
}

#[derive(Debug, thiserror::Error)]
pub enum WebCliError {
    #[error("horsies web requires the `web` cargo feature")]
    MissingFeature,
    #[error(
        "--auth none is only allowed on a loopback host; {host:?} is reachable from the network. Use --auth trusted-header with a proxy-set header."
    )]
    ExposedWithoutAuthentication { host: String },
    #[error("could not read configuration file {path}: {source}")]
    ConfigRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("could not parse TOML configuration file {path}: {source}")]
    ConfigParse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid application configuration: {0}")]
    ConfigInvalid(String),
    #[error("could not construct monitoring application: {0}")]
    App(#[from] crate::core::HorsiesError),
    #[error("could not connect to the monitoring database: {0}")]
    Broker(#[from] crate::AppError),
    #[cfg(feature = "web")]
    #[error("invalid trusted header name: {0}")]
    Header(#[from] axum::http::header::InvalidHeaderName),
    #[error("could not bind monitoring server to {host}:{port}: {source}")]
    Bind {
        host: String,
        port: u16,
        source: std::io::Error,
    },
    #[error("monitoring server failed: {0}")]
    Serve(std::io::Error),
}

impl WebCliError {
    pub const fn exit_code(&self) -> u8 {
        match self {
            Self::ExposedWithoutAuthentication { .. } => 2,
            #[cfg(feature = "web")]
            Self::Header(_) => 2,
            Self::MissingFeature
            | Self::ConfigRead { .. }
            | Self::ConfigParse { .. }
            | Self::ConfigInvalid(_)
            | Self::App(_)
            | Self::Broker(_)
            | Self::Bind { .. }
            | Self::Serve(_) => 1,
        }
    }
}

#[cfg(any(feature = "web", test))]
pub fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

#[cfg(feature = "web")]
fn load_config_file(path: &Path) -> Result<AppConfig, WebCliError> {
    let contents = std::fs::read_to_string(path).map_err(|source| WebCliError::ConfigRead {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&contents).map_err(|source| WebCliError::ConfigParse {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(feature = "web")]
fn resolve_config(args: &WebArgs) -> Result<AppConfig, WebCliError> {
    let mut config = match (&args.config, &args.database_url) {
        (Some(path), None) => load_config_file(path)?,
        (None, Some(url)) => AppConfig::for_database_url(url),
        (Some(_), Some(_)) | (None, None) => {
            return Err(WebCliError::ConfigInvalid(
                "provide exactly one CONFIG or --database-url".to_owned(),
            ));
        }
    };
    if args.database_url.is_some() {
        if let Some(session_url) = &args.session_database_url {
            config.broker.session_database_url = Some(session_url.clone());
        }
        config.broker.pgbouncer_transaction_mode = args.pgbouncer_transaction_mode;
    }
    let errors = config.validate();
    if errors.is_empty() {
        Ok(config)
    } else {
        Err(WebCliError::ConfigInvalid(
            errors
                .into_iter()
                .map(|error| error.to_string())
                .collect::<Vec<_>>()
                .join("; "),
        ))
    }
}

#[cfg(not(feature = "web"))]
pub async fn execute_web(_args: WebArgs) -> Result<(), WebCliError> {
    Err(WebCliError::MissingFeature)
}

#[cfg(feature = "web")]
pub async fn execute_web(args: WebArgs) -> Result<(), WebCliError> {
    use crate::web::{
        create_monitoring_router, AllowAll, MonitoringUiConfig, TrustedHeader, ViewOnly,
    };
    use crate::Horsies;

    enum CliAuthPolicy {
        None,
        Trusted(TrustedHeader),
    }

    if args.auth == WebAuthMode::None && !is_loopback_host(&args.host) {
        return Err(WebCliError::ExposedWithoutAuthentication {
            host: args.host.clone(),
        });
    }
    let auth_policy = match args.auth {
        WebAuthMode::None => CliAuthPolicy::None,
        WebAuthMode::TrustedHeader => {
            let policy = TrustedHeader::new(&args.trusted_header, args.enable_actions)?;
            eprintln!(
                "SECURITY: the reverse proxy in front of this server MUST strip or overwrite the {} header on incoming requests. A proxy that forwards a client-supplied header makes this mode trivially spoofable, and horsies cannot detect that.",
                args.trusted_header
            );
            CliAuthPolicy::Trusted(policy)
        }
    };

    super::init_tracing(args.loglevel);
    let config = resolve_config(&args)?;
    let app = Horsies::new_observe_only(config)?;
    let broker = app.get_broker().await?;
    let ui_config = MonitoringUiConfig {
        custom_css_url: args.custom_css_url.clone(),
    };
    let router = match (auth_policy, args.enable_actions) {
        (CliAuthPolicy::None, true) => {
            create_monitoring_router(&app, broker, AllowAll, ui_config, true)
        }
        (CliAuthPolicy::None, false) => {
            create_monitoring_router(&app, broker, ViewOnly, ui_config, false)
        }
        (CliAuthPolicy::Trusted(policy), enable_actions) => {
            create_monitoring_router(&app, broker, policy, ui_config, enable_actions)
        }
    };

    let listener = tokio::net::TcpListener::bind((args.host.as_str(), args.port))
        .await
        .map_err(|source| WebCliError::Bind {
            host: args.host.clone(),
            port: args.port,
            source,
        })?;
    let address = listener.local_addr().map_err(WebCliError::Serve)?;
    println!("horsies web listening on http://{address}");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(WebCliError::Serve)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "web")]
    use std::io::Write;

    use clap::Parser;

    use super::{is_loopback_host, WebAuthMode};
    #[cfg(feature = "web")]
    use super::{resolve_config, WebArgs};
    #[cfg(feature = "web")]
    use crate::worker::cli::LogLevel;
    use crate::worker::cli::{Cli, Command};

    #[test]
    fn loopback_rule_is_exact_and_fails_closed() {
        for host in ["localhost", "LOCALHOST", "127.0.0.1", "::1"] {
            assert!(is_loopback_host(host), "{host}");
        }
        for host in ["0.0.0.0", "192.168.1.4", "example.com", "localhost.local"] {
            assert!(!is_loopback_host(host), "{host}");
        }
    }

    #[test]
    fn parses_the_complete_database_url_posture() {
        let cli = Cli::try_parse_from([
            "horsies",
            "web",
            "--database-url",
            "postgresql://pool/db",
            "--session-database-url",
            "postgresql://direct/db",
            "--pgbouncer-transaction-mode",
            "--host",
            "localhost",
            "--port",
            "9000",
            "--auth",
            "trusted-header",
            "--trusted-header",
            "X-Identity",
            "--enable-actions",
            "--custom-css-url",
            "https://example.test/horsies.css",
            "--loglevel",
            "warning",
        ])
        .expect("web arguments should parse");
        let Command::Web(args) = cli.command else {
            panic!("expected web command");
        };
        assert_eq!(args.auth, WebAuthMode::TrustedHeader);
        assert!(args.pgbouncer_transaction_mode);
        assert!(args.enable_actions);
        assert_eq!(args.port, 9000);
        assert_eq!(args.trusted_header, "X-Identity");
    }

    #[test]
    fn requires_exactly_one_config_source() {
        assert!(Cli::try_parse_from(["horsies", "web"]).is_err());
        assert!(Cli::try_parse_from([
            "horsies",
            "web",
            "config.toml",
            "--database-url",
            "postgresql://localhost/db",
        ])
        .is_err());
        assert!(Cli::try_parse_from(["horsies", "web", "config.toml"]).is_ok());
    }

    #[cfg(feature = "web")]
    #[test]
    fn positional_toml_config_preserves_its_broker_posture() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            file,
            "[broker]\ndatabase_url = \"postgresql://pool/horsies\"\nsession_database_url = \"postgresql://direct/horsies\"\npgbouncer_transaction_mode = true"
        )
        .unwrap();
        let config = resolve_config(&WebArgs {
            config: Some(file.path().to_path_buf()),
            database_url: None,
            session_database_url: None,
            pgbouncer_transaction_mode: false,
            host: "127.0.0.1".to_owned(),
            port: 8600,
            auth: WebAuthMode::None,
            trusted_header: "X-Forwarded-User".to_owned(),
            enable_actions: false,
            custom_css_url: None,
            loglevel: LogLevel::Info,
        })
        .unwrap();
        assert_eq!(config.broker.database_url, "postgresql://pool/horsies");
        assert_eq!(
            config.broker.session_database_url.as_deref(),
            Some("postgresql://direct/horsies")
        );
        assert!(config.broker.pgbouncer_transaction_mode);
        assert_eq!(config.broker.pool_size, 30);
    }
}

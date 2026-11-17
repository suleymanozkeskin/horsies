//! The connection plan of one URL, and opening one connection from it.
//!
//! [`ConnectPlan::load`] does the file and user-database reads once: TLS
//! files (named, or libpq's defaults under `~/.postgresql/`), `~/.pgpass`
//! when the password comes from it, and the process user name when the URL
//! names no user. [`open_connection`] then only connects.

use std::fmt;
use std::time::Duration;

use tokio_postgres::config::{
    ChannelBinding, LoadBalanceHosts, SslMode as DriverSslMode, SslNegotiation, TargetSessionAttrs,
};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{Client, Config, Socket};
use tokio_postgres_rustls::MakeRustlsConnect;

use super::environment::{
    lookup_password, parse_password_file, read_password_file, ConnectionEnvironment,
    EnvironmentPassword, HomeDirectory, PasswordFileEntry, PasswordFileError, PasswordFileMatch,
    PasswordFileRead, PasswordLookup,
};
use super::failure::{ConnectError, ConnectFailure, PlainRetry};
use super::tls::{load_tls, TlsLoadError, TlsNegotiation, TlsPolicy};
use super::url::{
    ChannelBindingPolicy, ConnectionSettings, DatabaseName, DatabasePassword, Endpoint,
    HostAddress, HostOrder, MaskedUrl, TargetSession, UrlPassword, UserName,
};

/// Idle time before the first TCP keepalive probe (Python's default).
pub const DEFAULT_KEEPALIVES_IDLE: Duration = Duration::from_secs(30);

/// Time between TCP keepalive probes (Python's default).
pub const DEFAULT_KEEPALIVES_INTERVAL: Duration = Duration::from_secs(10);

/// Unanswered keepalive probes before the kernel drops the connection
/// (Python's default).
pub const DEFAULT_KEEPALIVES_PROBES: u32 = 3;

/// Time that sent data may stay unacknowledged before the kernel closes the
/// connection (Python's `DEFAULT_TCP_USER_TIMEOUT_MS`). It bounds a write to a
/// silently dead connection, which keepalives do not probe.
pub const DEFAULT_TCP_USER_TIMEOUT: Duration = Duration::from_millis(30_000);

/// The host name a Unix socket connection also matches in `~/.pgpass`.
const LOCALHOST: &str = "localhost";

/// TCP keepalive settings of every core connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Keepalives {
    Off,
    On {
        idle: Duration,
        interval: Duration,
        probes: u32,
    },
}

/// The TCP user timeout of every core connection. Linux only; other
/// platforms ignore it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpUserTimeout {
    SystemDefault,
    After(Duration),
}

/// Socket settings that the core sets on every connection. They come from
/// the core's configuration, never from the URL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SocketSettings {
    pub keepalives: Keepalives,
    pub tcp_user_timeout: TcpUserTimeout,
}

/// Python's defaults: keepalives on (30 s idle, 10 s interval, 3 probes) and
/// a 30 s TCP user timeout.
pub const DEFAULT_SOCKET_SETTINGS: SocketSettings = SocketSettings {
    keepalives: Keepalives::On {
        idle: DEFAULT_KEEPALIVES_IDLE,
        interval: DEFAULT_KEEPALIVES_INTERVAL,
        probes: DEFAULT_KEEPALIVES_PROBES,
    },
    tcp_user_timeout: TcpUserTimeout::After(DEFAULT_TCP_USER_TIMEOUT),
};

type TlsStream = <MakeRustlsConnect as MakeTlsConnect<Socket>>::Stream;

/// The driver half of a connection. It must be polled for the client to make
/// progress: pooled connections spawn it; the listener polls it to read
/// notifications.
pub type DriverConnection = tokio_postgres::Connection<Socket, TlsStream>;

/// The driver configurations of one URL, in the order they are tried.
enum ConnectAttempts {
    Single(Config),
    /// `sslmode=allow`: plain first; TLS when the server refuses plain.
    PlainThenTls {
        plain: Config,
        tls: Config,
    },
    /// `sslmode=prefer`: TLS when the server offers it; plain when the TLS
    /// handshake fails or the server refuses the session.
    TlsThenPlain {
        tls: Config,
        plain: Config,
    },
}

/// Everything needed to open connections to one URL. Built once by
/// [`ConnectPlan::load`]; opening a connection reads no file.
pub struct ConnectPlan {
    attempts: ConnectAttempts,
    tls: MakeRustlsConnect,
    masked_url: MaskedUrl,
    attempt_bound: Duration,
}

impl fmt::Debug for ConnectPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectPlan")
            .field("masked_url", &self.masked_url)
            .field("attempt_bound", &self.attempt_bound)
            .finish_non_exhaustive()
    }
}

/// Loading a connection plan failed. Nothing was connected.
#[derive(Debug, thiserror::Error)]
pub enum ConnectPlanError {
    #[error("{0}")]
    Tls(TlsLoadError),
    #[error("{0}")]
    PasswordFile(PasswordFileError),
    #[error("the URL names no user and the process user name could not be read: {cause}")]
    ProcessUser { cause: String },
    #[error(
        "~/.pgpass gives different passwords (or a password for only some) of the hosts of \
         {masked_url}; one URL connects with one password"
    )]
    PasswordDiffersPerHost { masked_url: MaskedUrl },
}

/// The password a plan connects with.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ResolvedPassword {
    Given(DatabasePassword),
    NoPassword,
}

impl ConnectPlan {
    /// Builds the plan of one URL.
    ///
    /// Reads (blocking): the TLS files the URL names; `~/.pgpass` when neither
    /// the URL nor `PGPASSWORD` gives a password; the process user name when
    /// the URL names no user. Connects to nothing.
    pub fn load(
        settings: &ConnectionSettings,
        environment: &ConnectionEnvironment,
        socket: &SocketSettings,
    ) -> Result<ConnectPlan, ConnectPlanError> {
        let user = resolve_user(settings.user())?;
        let database = match settings.database() {
            DatabaseName::Named(name) => name.clone(),
            DatabaseName::SameAsUser => user.clone(),
        };
        let password = resolve_password(settings, environment, &user, &database)?;
        let loaded = load_tls(settings.tls(), environment.home()).map_err(ConnectPlanError::Tls)?;
        let base = base_config(settings, &user, &database, &password, socket);
        let attempts = match &loaded.policy {
            TlsPolicy::Disabled => {
                ConnectAttempts::Single(with_ssl_mode(&base, DriverSslMode::Disable))
            }
            TlsPolicy::PlainThenTls(_) => ConnectAttempts::PlainThenTls {
                plain: with_ssl_mode(&base, DriverSslMode::Disable),
                tls: with_ssl_mode(&base, DriverSslMode::Require),
            },
            TlsPolicy::TlsThenPlain(_) => ConnectAttempts::TlsThenPlain {
                tls: with_ssl_mode(&base, DriverSslMode::Prefer),
                plain: with_ssl_mode(&base, DriverSslMode::Disable),
            },
            TlsPolicy::TlsOnly(_) => {
                ConnectAttempts::Single(with_ssl_mode(&base, DriverSslMode::Require))
            }
        };
        let host_count = u32::try_from(settings.endpoints().len()).unwrap_or(u32::MAX);
        Ok(ConnectPlan {
            attempts,
            tls: loaded.connector,
            masked_url: settings.masked_url().clone(),
            attempt_bound: settings.connect_timeout().saturating_mul(host_count),
        })
    }

    /// The URL of the plan, with the password masked.
    pub fn masked_url(&self) -> &MaskedUrl {
        &self.masked_url
    }

    /// The bound of one whole connection attempt: the socket connect, TLS,
    /// startup and authentication, over every host of the URL.
    pub fn attempt_bound(&self) -> Duration {
        self.attempt_bound
    }

    /// The TLS connector, also used for cancel requests.
    pub(crate) fn tls(&self) -> &MakeRustlsConnect {
        &self.tls
    }

    /// The driver configuration of the first attempt.
    #[cfg(test)]
    pub(crate) fn first_attempt(&self) -> &Config {
        match &self.attempts {
            ConnectAttempts::Single(config)
            | ConnectAttempts::PlainThenTls { plain: config, .. }
            | ConnectAttempts::TlsThenPlain { tls: config, .. } => config,
        }
    }
}

/// Opens one connection.
///
/// Each attempt is bounded by [`ConnectPlan::attempt_bound`]. As libpq:
/// - `sslmode=allow`: a server refusal of the plain attempt (an error
///   response during startup) is followed by one TLS attempt.
/// - `sslmode=prefer`: a failed TLS handshake, or a server refusal of the
///   session, is followed by one plain attempt. The driver does not tell
///   whether a refused session used TLS, so a server that offers no TLS and
///   refuses the session gets the same plain attempt a second time.
///
/// Any other failure is returned as it is. The caller must poll the returned
/// driver connection.
pub async fn open_connection(
    plan: &ConnectPlan,
) -> Result<(Client, DriverConnection), ConnectError> {
    match &plan.attempts {
        ConnectAttempts::Single(config) => connect_once(plan, config).await,
        ConnectAttempts::PlainThenTls { plain, tls } => match connect_once(plan, plain).await {
            Ok(pair) => Ok(pair),
            Err(ConnectError {
                cause: ConnectFailure::ServerRefused(_),
                ..
            }) => connect_once(plan, tls).await,
            Err(other) => Err(other),
        },
        ConnectAttempts::TlsThenPlain { tls, plain } => match connect_once(plan, tls).await {
            Ok(pair) => Ok(pair),
            Err(error) => match error.plain_retry() {
                PlainRetry::Allowed => connect_once(plan, plain).await,
                PlainRetry::NotAllowed => Err(error),
            },
        },
    }
}

async fn connect_once(
    plan: &ConnectPlan,
    config: &Config,
) -> Result<(Client, DriverConnection), ConnectError> {
    match tokio::time::timeout(plan.attempt_bound, config.connect(plan.tls.clone())).await {
        Ok(Ok(pair)) => Ok(pair),
        Ok(Err(error)) => Err(ConnectError::from_driver(plan.masked_url.clone(), error)),
        Err(_elapsed) => Err(ConnectError {
            masked_url: plan.masked_url.clone(),
            cause: ConnectFailure::TimedOut {
                bound: plan.attempt_bound,
            },
        }),
    }
}

fn resolve_user(user: &UserName) -> Result<String, ConnectPlanError> {
    match user {
        UserName::Named(name) => Ok(name.clone()),
        UserName::ProcessUser => {
            whoami::username().map_err(|cause| ConnectPlanError::ProcessUser {
                cause: cause.to_string(),
            })
        }
    }
}

/// libpq's order: the URL, then `PGPASSWORD`, then `~/.pgpass`. An empty
/// `password=` in the query skips `PGPASSWORD`, as in libpq.
fn resolve_password(
    settings: &ConnectionSettings,
    environment: &ConnectionEnvironment,
    user: &str,
    database: &str,
) -> Result<ResolvedPassword, ConnectPlanError> {
    match (settings.password(), environment.password()) {
        (UrlPassword::Given(password), _) => Ok(ResolvedPassword::Given(password.clone())),
        (UrlPassword::NotGiven, EnvironmentPassword::Set(password)) => {
            Ok(ResolvedPassword::Given(password.clone()))
        }
        (UrlPassword::GivenEmpty, _) | (UrlPassword::NotGiven, EnvironmentPassword::NotSet) => {
            password_from_file(settings, environment.home(), user, database)
        }
    }
}

/// Looks up every host of the URL in `~/.pgpass`. All hosts must agree: the
/// driver sends one password to whichever host answers.
fn password_from_file(
    settings: &ConnectionSettings,
    home: &HomeDirectory,
    user: &str,
    database: &str,
) -> Result<ResolvedPassword, ConnectPlanError> {
    let text = match read_password_file(home).map_err(ConnectPlanError::PasswordFile)? {
        PasswordFileRead::Absent => return Ok(ResolvedPassword::NoPassword),
        PasswordFileRead::Contents { text, .. } => text,
    };
    let entries = parse_password_file(&text).map_err(ConnectPlanError::PasswordFile)?;
    let matches: Vec<PasswordFileMatch> = settings
        .endpoints()
        .iter()
        .map(|endpoint| lookup_for_endpoint(&entries, endpoint, user, database))
        .collect();
    let Some((first, rest)) = matches.split_first() else {
        return Ok(ResolvedPassword::NoPassword);
    };
    match (rest.iter().all(|other| other == first), first) {
        (true, PasswordFileMatch::Found(password)) => Ok(ResolvedPassword::Given(password.clone())),
        (true, PasswordFileMatch::NoMatch) => Ok(ResolvedPassword::NoPassword),
        (false, _) => Err(ConnectPlanError::PasswordDiffersPerHost {
            masked_url: settings.masked_url().clone(),
        }),
    }
}

/// A TCP host matches by its name. A Unix socket directory matches by its
/// path, then by `localhost` (libpq uses `localhost` for its default socket
/// directory, which differs per build).
fn lookup_for_endpoint(
    entries: &[PasswordFileEntry],
    endpoint: &Endpoint,
    user: &str,
    database: &str,
) -> PasswordFileMatch {
    let lookup_host = |host: &str| {
        lookup_password(
            entries,
            &PasswordLookup {
                host,
                port: endpoint.port,
                database,
                user,
            },
        )
    };
    match &endpoint.host {
        HostAddress::Tcp(name) => lookup_host(name),
        HostAddress::UnixSocketDirectory(path) => match lookup_host(path) {
            PasswordFileMatch::Found(password) => PasswordFileMatch::Found(password),
            PasswordFileMatch::NoMatch => lookup_host(LOCALHOST),
        },
    }
}

fn base_config(
    settings: &ConnectionSettings,
    user: &str,
    database: &str,
    password: &ResolvedPassword,
    socket: &SocketSettings,
) -> Config {
    let mut config = Config::new();
    for endpoint in settings.endpoints() {
        config.host(endpoint.host.as_str());
        config.port(endpoint.port);
    }
    config.user(user);
    config.dbname(database);
    match password {
        ResolvedPassword::Given(password) => {
            config.password(password.as_bytes());
        }
        ResolvedPassword::NoPassword => {}
    }
    if let Some(application_name) = settings.application_name() {
        config.application_name(application_name);
    }
    if let Some(options) = settings.options() {
        config.options(options);
    }
    config.connect_timeout(settings.connect_timeout());
    apply_socket_settings(&mut config, socket);
    config.target_session_attrs(match settings.target_session() {
        TargetSession::Any => TargetSessionAttrs::Any,
        TargetSession::ReadWrite => TargetSessionAttrs::ReadWrite,
        TargetSession::ReadOnly => TargetSessionAttrs::ReadOnly,
    });
    config.channel_binding(match settings.channel_binding() {
        ChannelBindingPolicy::Disable => ChannelBinding::Disable,
        ChannelBindingPolicy::Prefer => ChannelBinding::Prefer,
        ChannelBindingPolicy::Require => ChannelBinding::Require,
    });
    config.load_balance_hosts(match settings.host_order() {
        HostOrder::AsGiven => LoadBalanceHosts::Disable,
        HostOrder::Random => LoadBalanceHosts::Random,
    });
    config.ssl_negotiation(match settings.tls().negotiation() {
        TlsNegotiation::Postgres => SslNegotiation::Postgres,
        TlsNegotiation::Direct => SslNegotiation::Direct,
    });
    config
}

fn apply_socket_settings(config: &mut Config, socket: &SocketSettings) {
    match socket.keepalives {
        Keepalives::Off => {
            config.keepalives(false);
        }
        Keepalives::On {
            idle,
            interval,
            probes,
        } => {
            config.keepalives(true);
            config.keepalives_idle(idle);
            config.keepalives_interval(interval);
            config.keepalives_retries(probes);
        }
    }
    match socket.tcp_user_timeout {
        TcpUserTimeout::SystemDefault => {}
        TcpUserTimeout::After(timeout) => {
            config.tcp_user_timeout(timeout);
        }
    }
}

fn with_ssl_mode(base: &Config, mode: DriverSslMode) -> Config {
    let mut config = base.clone();
    config.ssl_mode(mode);
    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::url::parse_connection_url;

    fn settings(text: &str) -> ConnectionSettings {
        parse_connection_url(text, &ConnectionEnvironment::isolated()).expect("valid URL")
    }

    fn plan(text: &str) -> ConnectPlan {
        ConnectPlan::load(
            &settings(text),
            &ConnectionEnvironment::isolated(),
            &DEFAULT_SOCKET_SETTINGS,
        )
        .expect("plan loads")
    }

    /// The configuration of the first attempt.
    fn single_config(plan: &ConnectPlan) -> &Config {
        plan.first_attempt()
    }

    #[test]
    fn the_plan_carries_hosts_user_database_and_socket_settings() {
        let plan = plan("postgresql://alice:pw@a:5433,b/jobs?application_name=w");
        let config = single_config(&plan);
        assert_eq!(config.get_ports(), &[5433, 5432]);
        assert_eq!(config.get_user(), Some("alice"));
        assert_eq!(config.get_dbname(), Some("jobs"));
        assert_eq!(config.get_password(), Some(&b"pw"[..]));
        assert_eq!(config.get_application_name(), Some("w"));
        assert!(config.get_keepalives());
        assert_eq!(config.get_keepalives_idle(), DEFAULT_KEEPALIVES_IDLE);
        assert_eq!(
            config.get_keepalives_interval(),
            Some(DEFAULT_KEEPALIVES_INTERVAL)
        );
        assert_eq!(
            config.get_keepalives_retries(),
            Some(DEFAULT_KEEPALIVES_PROBES)
        );
        assert_eq!(
            config.get_tcp_user_timeout(),
            Some(&DEFAULT_TCP_USER_TIMEOUT)
        );
        assert_eq!(config.get_ssl_mode(), DriverSslMode::Prefer);
        assert_eq!(plan.attempt_bound(), Duration::from_secs(20));
    }

    #[test]
    fn the_database_defaults_to_the_user() {
        let plan = plan("postgresql://alice@h");
        assert_eq!(single_config(&plan).get_dbname(), Some("alice"));
    }

    #[test]
    fn allow_tries_plain_then_tls() {
        let plan = plan("postgresql://u@h/d?sslmode=allow");
        match &plan.attempts {
            ConnectAttempts::PlainThenTls { plain, tls } => {
                assert_eq!(plain.get_ssl_mode(), DriverSslMode::Disable);
                assert_eq!(tls.get_ssl_mode(), DriverSslMode::Require);
            }
            ConnectAttempts::Single(_) | ConnectAttempts::TlsThenPlain { .. } => {
                panic!("expected plain, then TLS")
            }
        }
    }

    #[test]
    fn prefer_tries_tls_then_plain() {
        let plan = plan("postgresql://u@h/d?sslmode=prefer");
        match &plan.attempts {
            ConnectAttempts::TlsThenPlain { tls, plain } => {
                assert_eq!(tls.get_ssl_mode(), DriverSslMode::Prefer);
                assert_eq!(plain.get_ssl_mode(), DriverSslMode::Disable);
            }
            ConnectAttempts::Single(_) | ConnectAttempts::PlainThenTls { .. } => {
                panic!("expected TLS, then plain")
            }
        }
    }

    #[test]
    fn each_policy_maps_to_its_driver_mode() {
        for (sslmode, expected) in [
            ("disable", DriverSslMode::Disable),
            ("require", DriverSslMode::Require),
        ] {
            let plan = plan(&format!("postgresql://u@h/d?sslmode={sslmode}"));
            assert_eq!(single_config(&plan).get_ssl_mode(), expected, "{sslmode}");
        }
    }

    #[test]
    fn the_environment_password_is_used_when_the_url_has_none() {
        let environment = ConnectionEnvironment::new(
            EnvironmentPassword::Set(DatabasePassword::from_bytes(b"from-env".to_vec())),
            Vec::new(),
            HomeDirectory::Unknown,
        );
        let plan = ConnectPlan::load(
            &settings("postgresql://u@h/d"),
            &environment,
            &DEFAULT_SOCKET_SETTINGS,
        )
        .expect("plan loads");
        assert_eq!(single_config(&plan).get_password(), Some(&b"from-env"[..]));
    }

    #[test]
    fn the_url_password_wins_over_the_environment() {
        let environment = ConnectionEnvironment::new(
            EnvironmentPassword::Set(DatabasePassword::from_bytes(b"from-env".to_vec())),
            Vec::new(),
            HomeDirectory::Unknown,
        );
        let plan = ConnectPlan::load(
            &settings("postgresql://u:from-url@h/d"),
            &environment,
            &DEFAULT_SOCKET_SETTINGS,
        )
        .expect("plan loads");
        assert_eq!(single_config(&plan).get_password(), Some(&b"from-url"[..]));
    }

    #[cfg(unix)]
    #[test]
    fn the_password_file_must_agree_across_hosts() {
        use std::os::unix::fs::PermissionsExt;

        let home = std::env::temp_dir().join(format!("stmt_home_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&home).expect("create temp home");
        let path = home.join(".pgpass");
        std::fs::write(&path, "a:5432:d:u:one\nb:5432:d:u:two\n").expect("write password file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("mode");
        let environment = ConnectionEnvironment::new(
            EnvironmentPassword::NotSet,
            Vec::new(),
            HomeDirectory::Known(home.clone()),
        );
        let differing = ConnectPlan::load(
            &settings("postgresql://u@a,b/d"),
            &environment,
            &DEFAULT_SOCKET_SETTINGS,
        );
        let single = ConnectPlan::load(
            &settings("postgresql://u@b/d"),
            &environment,
            &DEFAULT_SOCKET_SETTINGS,
        );
        let _removed = std::fs::remove_dir_all(&home);
        assert!(matches!(
            differing,
            Err(ConnectPlanError::PasswordDiffersPerHost { .. })
        ));
        let single = single.expect("one host loads");
        assert_eq!(single_config(&single).get_password(), Some(&b"two"[..]));
    }

    #[test]
    fn keepalives_off_and_system_user_timeout() {
        let socket = SocketSettings {
            keepalives: Keepalives::Off,
            tcp_user_timeout: TcpUserTimeout::SystemDefault,
        };
        let plan = ConnectPlan::load(
            &settings("postgresql://u@h/d"),
            &ConnectionEnvironment::isolated(),
            &socket,
        )
        .expect("plan loads");
        let config = single_config(&plan);
        assert!(!config.get_keepalives());
        assert_eq!(config.get_tcp_user_timeout(), None);
    }

    #[test]
    fn debug_shows_only_the_masked_url() {
        let plan = plan("postgresql://u:secret@h/d");
        let text = format!("{plan:?}");
        assert!(text.contains("u:***@h"));
        assert!(!text.contains("secret"));
    }
}

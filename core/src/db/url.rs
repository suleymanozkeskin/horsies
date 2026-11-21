//! Connection-string parsing.
//!
//! Accepted form: `postgresql://` or `postgres://`, then
//! `[user[:password]@][host[:port][,host[:port]...]][/dbname][?name=value&...]`,
//! with libpq's percent-encoding. A host in brackets is an IPv6 address; a
//! host that starts with `/` is a Unix socket directory.
//!
//! Supported query parameters: `host`, `port`, `user`, `password`, `dbname`,
//! `application_name`, `options`, `connect_timeout`, `sslmode`, `sslrootcert`,
//! `sslcert`, `sslkey`, `sslnegotiation`, `channel_binding`,
//! `target_session_attrs`, `load_balance_hosts`.
//!
//! Refused, each by name:
//! - the keepalive parameters and `tcp_user_timeout`: the core sets them from
//!   its own configuration;
//! - every other libpq parameter (Kerberos and GSSAPI, service files,
//!   `passfile`, `sslpassword`, CRLs and the rest);
//! - every unknown parameter;
//! - a parameter given twice, also when the URL body already gives it;
//! - a set libpq environment variable that libpq would apply
//!   ([`VariableRule`]): a variable whose parameter the URL does not set, or a
//!   variable that horsies does not support. A variable whose parameter the
//!   URL sets is ignored, as libpq ignores it.
//!
//! A parameter counts as set when the query carries it (also with an empty
//! value) or when its part of the URL body is not empty, as in libpq.
//!
//! The parser does no I/O. It reads no file and no environment variable; the
//! caller passes the environment as a value.

use std::fmt;
use std::time::Duration;

use super::environment::{variable_rule, ConnectionEnvironment, EnvironmentPassword, VariableRule};
use super::tls::{
    ClientFile, RootCertificates, SslMode, SslModeSource, TlsNegotiation, TlsPolicyError,
    TlsSettings,
};

/// The port when the URL names none.
pub const DEFAULT_PORT: u16 = 5432;

/// The longest connection URL the parser reads.
pub const MAX_URL_BYTES: usize = 8192;

/// The most hosts one URL may list.
pub const MAX_HOSTS: usize = 16;

/// The most query parameters one URL may carry.
pub const MAX_QUERY_PARAMETERS: usize = 32;

/// The bound of one connection attempt per host when the URL sets no
/// `connect_timeout`. libpq waits without a bound; the core does not.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The largest `connect_timeout` the parser accepts, in seconds.
pub const MAX_CONNECT_TIMEOUT_SECONDS: u64 = 3600;

const ACCEPTED_SCHEMES: [&str; 2] = ["postgresql", "postgres"];

/// libpq parameters that the core sets from its own configuration.
const SET_BY_CORE_PARAMETERS: &[&str] = &[
    "keepalives",
    "keepalives_idle",
    "keepalives_interval",
    "keepalives_count",
    "tcp_user_timeout",
];

/// libpq parameters that the core does not support.
const NOT_SUPPORTED_PARAMETERS: &[&str] = &[
    "hostaddr",
    "passfile",
    "service",
    "sslpassword",
    "sslcrl",
    "sslcrldir",
    "sslsni",
    "sslcompression",
    "sslcertmode",
    "ssl_min_protocol_version",
    "ssl_max_protocol_version",
    "requiressl",
    "requirepeer",
    "require_auth",
    "gssencmode",
    "gsslib",
    "gssdelegation",
    "krbsrvname",
    "client_encoding",
    "fallback_application_name",
    "replication",
    "min_protocol_version",
    "max_protocol_version",
    "oauth_issuer",
    "oauth_client_id",
    "oauth_client_secret",
    "oauth_scope",
];

/// A connection URL with its password masked, for errors and logs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaskedUrl(String);

impl MaskedUrl {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn for_tests(text: &str) -> MaskedUrl {
        MaskedUrl(text.to_owned())
    }
}

impl fmt::Display for MaskedUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A password. `Debug` never shows it.
#[derive(Clone, PartialEq, Eq)]
pub struct DatabasePassword(Vec<u8>);

impl DatabasePassword {
    pub fn from_bytes(bytes: Vec<u8>) -> DatabasePassword {
        DatabasePassword(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for DatabasePassword {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DatabasePassword(***)")
    }
}

/// The user name of the URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserName {
    Named(String),
    /// No user in the URL: the operating-system user of the process, as libpq.
    ProcessUser,
}

/// The database name of the URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseName {
    Named(String),
    /// No database in the URL: the user name, as libpq.
    SameAsUser,
}

/// The password of the URL, with libpq's reading of each case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UrlPassword {
    Given(DatabasePassword),
    /// `password=` with an empty value in the query: libpq skips
    /// `PGPASSWORD` and reads `~/.pgpass`.
    GivenEmpty,
    /// No password, or an empty one in the URL body: libpq reads
    /// `PGPASSWORD`, then `~/.pgpass`.
    NotGiven,
}

/// One host of the URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostAddress {
    /// A host name or an IP address.
    Tcp(String),
    /// A directory that holds the server's Unix socket.
    UnixSocketDirectory(String),
}

impl HostAddress {
    /// The text the driver takes: a host name, an address, or a directory
    /// path that starts with `/`.
    pub fn as_str(&self) -> &str {
        match self {
            HostAddress::Tcp(text) | HostAddress::UnixSocketDirectory(text) => text,
        }
    }

    fn from_text(text: String) -> HostAddress {
        match text.starts_with('/') {
            true => HostAddress::UnixSocketDirectory(text),
            false => HostAddress::Tcp(text),
        }
    }
}

/// One host and its port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub host: HostAddress,
    pub port: u16,
}

/// The `target_session_attrs` parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetSession {
    Any,
    ReadWrite,
    ReadOnly,
}

/// The `channel_binding` parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelBindingPolicy {
    Disable,
    Prefer,
    Require,
}

/// The `load_balance_hosts` parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostOrder {
    AsGiven,
    Random,
}

/// A parsed connection URL. Only [`parse_connection_url`] builds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionSettings {
    endpoints: Vec<Endpoint>,
    user: UserName,
    password: UrlPassword,
    database: DatabaseName,
    application_name: Option<String>,
    options: Option<String>,
    connect_timeout: Duration,
    tls: TlsSettings,
    target_session: TargetSession,
    channel_binding: ChannelBindingPolicy,
    host_order: HostOrder,
    masked_url: MaskedUrl,
}

impl ConnectionSettings {
    /// One to [`MAX_HOSTS`] hosts, in URL order.
    pub fn endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }

    pub fn user(&self) -> &UserName {
        &self.user
    }

    pub fn password(&self) -> &UrlPassword {
        &self.password
    }

    pub fn database(&self) -> &DatabaseName {
        &self.database
    }

    pub fn application_name(&self) -> Option<&str> {
        self.application_name.as_deref()
    }

    pub fn options(&self) -> Option<&str> {
        self.options.as_deref()
    }

    /// The bound of one socket connect per host.
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// These settings with `timeout` as the bound of one connect per host,
    /// in place of the URL's `connect_timeout` (as a libpq keyword argument
    /// overrides the same parameter of a conninfo).
    pub fn with_connect_timeout(self, timeout: Duration) -> ConnectionSettings {
        ConnectionSettings {
            connect_timeout: timeout,
            ..self
        }
    }

    pub fn tls(&self) -> &TlsSettings {
        &self.tls
    }

    pub fn target_session(&self) -> TargetSession {
        self.target_session
    }

    pub fn channel_binding(&self) -> ChannelBindingPolicy {
        self.channel_binding
    }

    pub fn host_order(&self) -> HostOrder {
        self.host_order
    }

    pub fn masked_url(&self) -> &MaskedUrl {
        &self.masked_url
    }
}

/// The part of a URL that a percent-decoding error refers to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UrlComponent {
    User,
    Password,
    Host,
    Database,
    ParameterName,
    ParameterValue { name: String },
}

impl fmt::Display for UrlComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UrlComponent::User => formatter.write_str("the user name"),
            UrlComponent::Password => formatter.write_str("the password"),
            UrlComponent::Host => formatter.write_str("a host"),
            UrlComponent::Database => formatter.write_str("the database name"),
            UrlComponent::ParameterName => formatter.write_str("a parameter name"),
            UrlComponent::ParameterValue { name } => write!(formatter, "the value of {name}"),
        }
    }
}

/// Why a query parameter is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnsupportedReason {
    /// The core sets this setting from its own configuration.
    SetByCore,
    /// A libpq parameter that the core does not support.
    NotSupported,
    /// Not a libpq parameter.
    Unknown,
}

impl fmt::Display for UnsupportedReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            UnsupportedReason::SetByCore => {
                "the core sets it from its own configuration; remove it from the URL"
            }
            UnsupportedReason::NotSupported => "horsies does not support this libpq parameter",
            UnsupportedReason::Unknown => "it is not a libpq parameter",
        })
    }
}

/// A connection URL that the core refuses. Every variant names what to fix.
/// No variant carries the password.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum UrlError {
    #[error("the connection URL is {length} bytes; the limit is {limit}")]
    TooLong { length: usize, limit: usize },
    #[error("the connection URL must start with postgresql:// or postgres://")]
    NotAUrl,
    #[error(
        "the URL scheme {scheme}:// is not supported; use postgresql:// or postgres:// \
         (a driver suffix such as +psycopg is not part of the URL the core reads)"
    )]
    UnsupportedScheme { scheme: String },
    #[error("{component} has an invalid percent-encoding")]
    InvalidPercentEncoding { component: UrlComponent },
    #[error("{component} is not valid UTF-8 after percent-decoding")]
    NotUtf8 { component: UrlComponent },
    #[error(
        "the URL names no host; give a host name, or host=/path/to/socket/directory for a Unix \
         socket"
    )]
    HostMissing,
    #[error("the URL lists {count} hosts; the limit is {limit}")]
    TooManyHosts { count: usize, limit: usize },
    #[error("an IPv6 host in the URL has no closing bracket")]
    UnclosedIpv6Bracket,
    #[error("the text after an IPv6 host must be :port")]
    TextAfterIpv6Host,
    #[error("the port {value:?} is not a number from 1 to 65535")]
    InvalidPort { value: String },
    #[error("the URL lists {hosts} hosts and {ports} ports; give one port, or one per host")]
    PortCountMismatch { hosts: usize, ports: usize },
    #[error("query parameter number {position} has no '='")]
    MalformedParameter { position: usize },
    #[error("the URL carries {count} query parameters; the limit is {limit}")]
    TooManyParameters { count: usize, limit: usize },
    #[error("the parameter {name} is given more than once")]
    DuplicateParameter { name: String },
    #[error("the parameter {name} is refused: {reason}")]
    UnsupportedParameter {
        name: String,
        reason: UnsupportedReason,
    },
    #[error("the value {value:?} of {name} is not supported")]
    UnsupportedValue { name: &'static str, value: String },
    #[error(
        "connect_timeout={seconds} waits without a bound; give a whole number of seconds from 1 \
         to {limit}"
    )]
    UnboundedConnectTimeout { seconds: i64, limit: u64 },
    #[error("connect_timeout={seconds} is above the limit of {limit} seconds")]
    ConnectTimeoutTooLong { seconds: u64, limit: u64 },
    #[error("{0}")]
    Tls(TlsPolicyError),
    #[error(
        "the environment variable {name} is set and the URL does not set {parameter}, so libpq \
         would use it; set {parameter} in the URL or unset {name}"
    )]
    EnvironmentDefault {
        name: &'static str,
        parameter: &'static str,
    },
    #[error(
        "the environment variable {name} is set and libpq would apply it; horsies does not \
         support it, so unset {name}"
    )]
    EnvironmentNotSupported { name: &'static str },
    #[error(
        "the environment variable {name} is set and the password comes from the password file; \
         horsies reads only ~/.pgpass, so unset {name} or give the password in the URL or in \
         PGPASSWORD"
    )]
    EnvironmentPasswordFile { name: &'static str },
}

/// Parses a connection URL.
///
/// Inputs: the URL text and the environment (its set libpq variables and
/// whether `PGPASSWORD` is set). Outcome: the typed settings, or the first
/// [`UrlError`]. No side effects: no file, no environment read, no network.
pub fn parse_connection_url(
    text: &str,
    environment: &ConnectionEnvironment,
) -> Result<ConnectionSettings, UrlError> {
    if text.len() > MAX_URL_BYTES {
        return Err(UrlError::TooLong {
            length: text.len(),
            limit: MAX_URL_BYTES,
        });
    }
    let (scheme, rest) = split_scheme(text)?;
    let parts = split_parts(rest);
    let credentials = parse_userinfo(parts.userinfo)?;
    let authority_hosts = parse_hostspec(parts.hostspec)?;
    let path_database = decode_text(parts.path, &UrlComponent::Database)?;
    let query = parse_query(parts.query)?;
    let masked_url = MaskedUrl(format!(
        "{scheme}://{}{}{}{}",
        masked_userinfo(parts.userinfo),
        parts.hostspec,
        parts.path_with_slash,
        query.masked,
    ));
    let named = set_parameters(
        &credentials,
        &authority_hosts,
        &path_database,
        &query.parameters,
    );
    let settings = assemble(
        credentials,
        authority_hosts,
        path_database,
        query.parameters,
        masked_url,
    )?;
    check_environment(environment, &named, &settings.password)?;
    Ok(settings)
}

/// The parameters the URL sets, in libpq's sense: a query parameter (also
/// with an empty value), or a part of the URL body that is not empty.
fn set_parameters(
    credentials: &Credentials,
    authority_hosts: &[RawHost],
    path_database: &str,
    parameters: &QueryParameters,
) -> Vec<&'static str> {
    let from_body = [
        ("host", !authority_hosts.is_empty()),
        (
            "port",
            authority_hosts
                .iter()
                .any(|host| matches!(host.port, PortSpec::Given(_))),
        ),
        ("user", matches!(credentials.user, UserName::Named(_))),
        (
            "password",
            matches!(credentials.password, UrlPassword::Given(_)),
        ),
        ("dbname", !path_database.is_empty()),
    ];
    let from_query = [
        ("host", parameters.host.is_some()),
        ("port", parameters.port.is_some()),
        ("user", parameters.user.is_some()),
        ("password", parameters.password.is_some()),
        ("dbname", parameters.dbname.is_some()),
        ("application_name", parameters.application_name.is_some()),
        ("options", parameters.options.is_some()),
        ("connect_timeout", parameters.connect_timeout.is_some()),
        ("sslmode", parameters.sslmode.is_some()),
        ("sslrootcert", parameters.sslrootcert.is_some()),
        ("sslcert", parameters.sslcert.is_some()),
        ("sslkey", parameters.sslkey.is_some()),
        ("sslnegotiation", parameters.sslnegotiation.is_some()),
        ("channel_binding", parameters.channel_binding.is_some()),
        (
            "target_session_attrs",
            parameters.target_session_attrs.is_some(),
        ),
        (
            "load_balance_hosts",
            parameters.load_balance_hosts.is_some(),
        ),
    ];
    from_body
        .into_iter()
        .chain(from_query)
        .filter(|(_, set)| *set)
        .map(|(name, _)| name)
        .collect()
}

/// Whether the password file is read: libpq reads it when the URL gives an
/// empty `password=`, or when neither the URL nor `PGPASSWORD` gives one.
enum PasswordFileUse {
    Read,
    NotRead,
}

fn password_file_use(
    password: &UrlPassword,
    environment: &ConnectionEnvironment,
) -> PasswordFileUse {
    match (password, environment.password()) {
        (UrlPassword::Given(_), _) | (UrlPassword::NotGiven, EnvironmentPassword::Set(_)) => {
            PasswordFileUse::NotRead
        }
        (UrlPassword::GivenEmpty, _) | (UrlPassword::NotGiven, EnvironmentPassword::NotSet) => {
            PasswordFileUse::Read
        }
    }
}

/// Refuses every set libpq variable that libpq would apply to this URL.
fn check_environment(
    environment: &ConnectionEnvironment,
    set: &[&'static str],
    password: &UrlPassword,
) -> Result<(), UrlError> {
    for &name in environment.set_libpq_variables() {
        match (
            variable_rule(name),
            password_file_use(password, environment),
        ) {
            (VariableRule::DefaultFor(parameter), _) if set.contains(&parameter) => {}
            (VariableRule::DefaultFor(parameter), _) => {
                return Err(UrlError::EnvironmentDefault { name, parameter })
            }
            (VariableRule::AlwaysApplies, _) => {
                return Err(UrlError::EnvironmentNotSupported { name })
            }
            (VariableRule::PasswordFileLocation, PasswordFileUse::Read) => {
                return Err(UrlError::EnvironmentPasswordFile { name })
            }
            (VariableRule::PasswordFileLocation, PasswordFileUse::NotRead)
            | (VariableRule::NoConnectionEffect, _) => {}
        }
    }
    Ok(())
}

struct UrlParts<'a> {
    userinfo: Option<&'a str>,
    hostspec: &'a str,
    /// The path as written, with its leading `/` (empty when absent).
    path_with_slash: &'a str,
    /// The path without its leading `/`.
    path: &'a str,
    query: Option<&'a str>,
}

fn split_scheme(text: &str) -> Result<(&str, &str), UrlError> {
    let Some((scheme, rest)) = text.split_once("://") else {
        return Err(UrlError::NotAUrl);
    };
    let looks_like_scheme = !scheme.is_empty()
        && scheme
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "+-.".contains(character));
    match (ACCEPTED_SCHEMES.contains(&scheme), looks_like_scheme) {
        (true, _) => Ok((scheme, rest)),
        (false, true) => Err(UrlError::UnsupportedScheme {
            scheme: scheme.to_owned(),
        }),
        (false, false) => Err(UrlError::NotAUrl),
    }
}

/// Splits the text after `://`. The user info ends at the first `@` before
/// the first `/` or `?` (libpq's rule: a `@` in a password is percent-encoded).
fn split_parts(rest: &str) -> UrlParts<'_> {
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, remainder) = rest.split_at(authority_end);
    let (userinfo, hostspec) = match authority.split_once('@') {
        Some((userinfo, hostspec)) => (Some(userinfo), hostspec),
        None => (None, authority),
    };
    let (path_with_slash, query) = match remainder.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (remainder, None),
    };
    let path = path_with_slash.strip_prefix('/').unwrap_or(path_with_slash);
    UrlParts {
        userinfo,
        hostspec,
        path_with_slash,
        path,
        query,
    }
}

fn masked_userinfo(userinfo: Option<&str>) -> String {
    match userinfo {
        None => String::new(),
        Some(userinfo) => match userinfo.split_once(':') {
            Some((user, _password)) => format!("{user}:***@"),
            None => format!("{userinfo}@"),
        },
    }
}

struct Credentials {
    user: UserName,
    password: UrlPassword,
}

fn parse_userinfo(userinfo: Option<&str>) -> Result<Credentials, UrlError> {
    let Some(userinfo) = userinfo else {
        return Ok(Credentials {
            user: UserName::ProcessUser,
            password: UrlPassword::NotGiven,
        });
    };
    let (user_text, password_text) = match userinfo.split_once(':') {
        Some((user, password)) => (user, Some(password)),
        None => (userinfo, None),
    };
    let user = decode_text(user_text, &UrlComponent::User)?;
    let password = match password_text {
        None => UrlPassword::NotGiven,
        Some(text) => password_from_bytes(percent_decode(text, &UrlComponent::Password)?),
    };
    Ok(Credentials {
        user: user_from_text(user),
        password,
    })
}

/// An empty user means the process user, as in libpq.
fn user_from_text(text: String) -> UserName {
    match text.is_empty() {
        true => UserName::ProcessUser,
        false => UserName::Named(text),
    }
}

/// An empty password means no password, as in libpq.
fn password_from_bytes(bytes: Vec<u8>) -> UrlPassword {
    match bytes.is_empty() {
        true => UrlPassword::NotGiven,
        false => UrlPassword::Given(DatabasePassword(bytes)),
    }
}

/// A port as written: given, or left to the default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PortSpec {
    Given(u16),
    Default,
}

impl PortSpec {
    fn port(self) -> u16 {
        match self {
            PortSpec::Given(port) => port,
            PortSpec::Default => DEFAULT_PORT,
        }
    }
}

struct RawHost {
    name: String,
    port: PortSpec,
}

fn parse_hostspec(hostspec: &str) -> Result<Vec<RawHost>, UrlError> {
    if hostspec.is_empty() {
        return Ok(Vec::new());
    }
    let count = hostspec.split(',').count();
    if count > MAX_HOSTS {
        return Err(UrlError::TooManyHosts {
            count,
            limit: MAX_HOSTS,
        });
    }
    hostspec.split(',').map(parse_host_piece).collect()
}

fn parse_host_piece(piece: &str) -> Result<RawHost, UrlError> {
    let (host_text, port_text) = match piece.strip_prefix('[') {
        Some(bracketed) => {
            let Some((inside, after)) = bracketed.split_once(']') else {
                return Err(UrlError::UnclosedIpv6Bracket);
            };
            match (after, after.strip_prefix(':')) {
                ("", _) => (inside, None),
                (_, Some(port)) => (inside, Some(port)),
                (_, None) => return Err(UrlError::TextAfterIpv6Host),
            }
        }
        None => match piece.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (piece, None),
        },
    };
    let name = decode_text(host_text, &UrlComponent::Host)?;
    if name.is_empty() {
        return Err(UrlError::HostMissing);
    }
    let port = match port_text {
        None => PortSpec::Default,
        Some(text) => parse_port(text)?,
    };
    Ok(RawHost { name, port })
}

fn parse_port(text: &str) -> Result<PortSpec, UrlError> {
    match (text, text.parse::<u16>()) {
        ("", _) => Ok(PortSpec::Default),
        (_, Ok(0) | Err(_)) => Err(UrlError::InvalidPort {
            value: text.to_owned(),
        }),
        (_, Ok(port)) => Ok(PortSpec::Given(port)),
    }
}

/// Every supported query parameter, at most once each, percent-decoded.
#[derive(Default)]
struct QueryParameters {
    host: Option<String>,
    port: Option<String>,
    user: Option<String>,
    password: Option<Vec<u8>>,
    dbname: Option<String>,
    application_name: Option<String>,
    options: Option<String>,
    connect_timeout: Option<String>,
    sslmode: Option<String>,
    sslrootcert: Option<String>,
    sslcert: Option<String>,
    sslkey: Option<String>,
    sslnegotiation: Option<String>,
    channel_binding: Option<String>,
    target_session_attrs: Option<String>,
    load_balance_hosts: Option<String>,
}

struct ParsedQuery {
    parameters: QueryParameters,
    /// `?` and the query as written, with the `password` value masked; empty
    /// when the URL has no query.
    masked: String,
}

fn parse_query(query: Option<&str>) -> Result<ParsedQuery, UrlError> {
    let mut parameters = QueryParameters::default();
    let Some(query) = query else {
        return Ok(ParsedQuery {
            parameters,
            masked: String::new(),
        });
    };
    let pieces: Vec<&str> = query.split('&').filter(|piece| !piece.is_empty()).collect();
    if pieces.len() > MAX_QUERY_PARAMETERS {
        return Err(UrlError::TooManyParameters {
            count: pieces.len(),
            limit: MAX_QUERY_PARAMETERS,
        });
    }
    let mut masked_pieces = Vec::with_capacity(pieces.len());
    for (index, piece) in pieces.iter().enumerate() {
        let Some((raw_name, raw_value)) = piece.split_once('=') else {
            return Err(UrlError::MalformedParameter {
                position: index + 1,
            });
        };
        let name = decode_text(raw_name, &UrlComponent::ParameterName)?;
        masked_pieces.push(match name.as_str() {
            "password" => "password=***".to_owned(),
            _ => (*piece).to_owned(),
        });
        store_parameter(&mut parameters, &name, raw_value)?;
    }
    Ok(ParsedQuery {
        parameters,
        masked: format!("?{}", masked_pieces.join("&")),
    })
}

fn store_parameter(
    parameters: &mut QueryParameters,
    name: &str,
    raw_value: &str,
) -> Result<(), UrlError> {
    let component = UrlComponent::ParameterValue {
        name: name.to_owned(),
    };
    let slot = match name {
        "password" => {
            let value = percent_decode(raw_value, &component)?;
            return store(&mut parameters.password, name, value);
        }
        "host" => &mut parameters.host,
        "port" => &mut parameters.port,
        "user" => &mut parameters.user,
        "dbname" => &mut parameters.dbname,
        "application_name" => &mut parameters.application_name,
        "options" => &mut parameters.options,
        "connect_timeout" => &mut parameters.connect_timeout,
        "sslmode" => &mut parameters.sslmode,
        "sslrootcert" => &mut parameters.sslrootcert,
        "sslcert" => &mut parameters.sslcert,
        "sslkey" => &mut parameters.sslkey,
        "sslnegotiation" => &mut parameters.sslnegotiation,
        "channel_binding" => &mut parameters.channel_binding,
        "target_session_attrs" => &mut parameters.target_session_attrs,
        "load_balance_hosts" => &mut parameters.load_balance_hosts,
        other => {
            return Err(UrlError::UnsupportedParameter {
                name: other.to_owned(),
                reason: unsupported_reason(other),
            })
        }
    };
    let value = decode_text(raw_value, &component)?;
    store(slot, name, value)
}

fn unsupported_reason(name: &str) -> UnsupportedReason {
    match (
        SET_BY_CORE_PARAMETERS.contains(&name),
        NOT_SUPPORTED_PARAMETERS.contains(&name),
    ) {
        (true, _) => UnsupportedReason::SetByCore,
        (false, true) => UnsupportedReason::NotSupported,
        (false, false) => UnsupportedReason::Unknown,
    }
}

fn store<T>(slot: &mut Option<T>, name: &str, value: T) -> Result<(), UrlError> {
    match slot {
        Some(_) => Err(UrlError::DuplicateParameter {
            name: name.to_owned(),
        }),
        None => {
            *slot = Some(value);
            Ok(())
        }
    }
}

/// Combines the URL body and the query parameters into the settings.
fn assemble(
    credentials: Credentials,
    authority_hosts: Vec<RawHost>,
    path_database: String,
    parameters: QueryParameters,
    masked_url: MaskedUrl,
) -> Result<ConnectionSettings, UrlError> {
    let endpoints = endpoints(authority_hosts, parameters.host, parameters.port)?;
    let user = match (credentials.user, parameters.user) {
        (UserName::Named(_), Some(_)) => return Err(duplicate("user")),
        (UserName::Named(name), None) => UserName::Named(name),
        (UserName::ProcessUser, Some(text)) => user_from_text(text),
        (UserName::ProcessUser, None) => UserName::ProcessUser,
    };
    let password = match (credentials.password, parameters.password) {
        (UrlPassword::Given(_), Some(_)) => return Err(duplicate("password")),
        (UrlPassword::Given(password), None) => UrlPassword::Given(password),
        (UrlPassword::GivenEmpty | UrlPassword::NotGiven, Some(bytes)) => match bytes.is_empty() {
            true => UrlPassword::GivenEmpty,
            false => UrlPassword::Given(DatabasePassword(bytes)),
        },
        (password @ (UrlPassword::GivenEmpty | UrlPassword::NotGiven), None) => password,
    };
    let database = match (path_database.is_empty(), parameters.dbname) {
        (false, Some(_)) => return Err(duplicate("dbname")),
        (false, None) => DatabaseName::Named(path_database),
        (true, Some(name)) if !name.is_empty() => DatabaseName::Named(name),
        (true, Some(_) | None) => DatabaseName::SameAsUser,
    };
    let tls = TlsSettings::resolve(
        sslmode_source(parameters.sslmode)?,
        root_certificates(parameters.sslrootcert.as_deref()),
        client_file(parameters.sslcert.as_deref()),
        client_file(parameters.sslkey.as_deref()),
        tls_negotiation(parameters.sslnegotiation.as_deref())?,
    )
    .map_err(UrlError::Tls)?;
    Ok(ConnectionSettings {
        endpoints,
        user,
        password,
        database,
        application_name: parameters.application_name,
        options: parameters.options,
        connect_timeout: connect_timeout(parameters.connect_timeout)?,
        tls,
        target_session: target_session(parameters.target_session_attrs.as_deref())?,
        channel_binding: channel_binding(parameters.channel_binding.as_deref())?,
        host_order: host_order(parameters.load_balance_hosts.as_deref())?,
        masked_url,
    })
}

fn duplicate(name: &str) -> UrlError {
    UrlError::DuplicateParameter {
        name: name.to_owned(),
    }
}

/// The hosts come from the URL body or from `host=`, never both. Ports come
/// from the body (`host:port`) or from `port=`, never both; `port=` gives one
/// port for every host, or one per host.
fn endpoints(
    authority_hosts: Vec<RawHost>,
    host_parameter: Option<String>,
    port_parameter: Option<String>,
) -> Result<Vec<Endpoint>, UrlError> {
    let hosts = match (authority_hosts.is_empty(), host_parameter) {
        (false, Some(_)) => return Err(duplicate("host")),
        (false, None) => authority_hosts,
        (true, Some(list)) => hosts_from_parameter(&list)?,
        (true, None) => return Err(UrlError::HostMissing),
    };
    let Some(port_list) = port_parameter else {
        return Ok(hosts
            .into_iter()
            .map(|host| Endpoint {
                host: HostAddress::from_text(host.name),
                port: host.port.port(),
            })
            .collect());
    };
    if hosts
        .iter()
        .any(|host| matches!(host.port, PortSpec::Given(_)))
    {
        return Err(duplicate("port"));
    }
    let ports = port_list
        .split(',')
        .map(parse_port)
        .collect::<Result<Vec<PortSpec>, UrlError>>()?;
    let host_count = hosts.len();
    let port_count = ports.len();
    let ports_per_host = match (ports.first().copied(), port_count) {
        (Some(single), 1) => vec![single; host_count],
        (_, count) if count == host_count => ports,
        (_, _) => {
            return Err(UrlError::PortCountMismatch {
                hosts: host_count,
                ports: port_count,
            })
        }
    };
    Ok(hosts
        .into_iter()
        .zip(ports_per_host)
        .map(|(host, port)| Endpoint {
            host: HostAddress::from_text(host.name),
            port: port.port(),
        })
        .collect())
}

fn hosts_from_parameter(list: &str) -> Result<Vec<RawHost>, UrlError> {
    let names: Vec<&str> = list.split(',').collect();
    if names.len() > MAX_HOSTS {
        return Err(UrlError::TooManyHosts {
            count: names.len(),
            limit: MAX_HOSTS,
        });
    }
    names
        .into_iter()
        .map(|name| match name.is_empty() {
            true => Err(UrlError::HostMissing),
            false => Ok(RawHost {
                name: name.to_owned(),
                port: PortSpec::Default,
            }),
        })
        .collect()
}

fn sslmode_source(value: Option<String>) -> Result<SslModeSource, UrlError> {
    match value {
        None => Ok(SslModeSource::Default),
        Some(text) => match SslMode::parse(&text) {
            Some(mode) => Ok(SslModeSource::Given(mode)),
            None => Err(UrlError::UnsupportedValue {
                name: "sslmode",
                value: text,
            }),
        },
    }
}

/// Unset or empty means libpq's default file, as in libpq.
fn root_certificates(value: Option<&str>) -> RootCertificates {
    match value {
        None | Some("") => RootCertificates::Default,
        Some("system") => RootCertificates::System,
        Some(path) => RootCertificates::File(std::path::PathBuf::from(path)),
    }
}

/// Unset or empty means libpq's default file, as in libpq.
fn client_file(value: Option<&str>) -> ClientFile {
    match value {
        None | Some("") => ClientFile::Default,
        Some(path) => ClientFile::File(std::path::PathBuf::from(path)),
    }
}

fn tls_negotiation(value: Option<&str>) -> Result<TlsNegotiation, UrlError> {
    match value {
        None | Some("postgres") => Ok(TlsNegotiation::Postgres),
        Some("direct") => Ok(TlsNegotiation::Direct),
        Some(other) => Err(UrlError::UnsupportedValue {
            name: "sslnegotiation",
            value: other.to_owned(),
        }),
    }
}

fn connect_timeout(value: Option<String>) -> Result<Duration, UrlError> {
    let Some(text) = value else {
        return Ok(DEFAULT_CONNECT_TIMEOUT);
    };
    let seconds = text
        .parse::<i64>()
        .map_err(|_| UrlError::UnsupportedValue {
            name: "connect_timeout",
            value: text.clone(),
        })?;
    match u64::try_from(seconds) {
        Ok(0) | Err(_) => Err(UrlError::UnboundedConnectTimeout {
            seconds,
            limit: MAX_CONNECT_TIMEOUT_SECONDS,
        }),
        Ok(positive) if positive > MAX_CONNECT_TIMEOUT_SECONDS => {
            Err(UrlError::ConnectTimeoutTooLong {
                seconds: positive,
                limit: MAX_CONNECT_TIMEOUT_SECONDS,
            })
        }
        Ok(positive) => Ok(Duration::from_secs(positive)),
    }
}

fn target_session(value: Option<&str>) -> Result<TargetSession, UrlError> {
    match value {
        None | Some("any") => Ok(TargetSession::Any),
        Some("read-write") => Ok(TargetSession::ReadWrite),
        Some("read-only") => Ok(TargetSession::ReadOnly),
        Some(other) => Err(UrlError::UnsupportedValue {
            name: "target_session_attrs",
            value: other.to_owned(),
        }),
    }
}

fn channel_binding(value: Option<&str>) -> Result<ChannelBindingPolicy, UrlError> {
    match value {
        None | Some("prefer") => Ok(ChannelBindingPolicy::Prefer),
        Some("disable") => Ok(ChannelBindingPolicy::Disable),
        Some("require") => Ok(ChannelBindingPolicy::Require),
        Some(other) => Err(UrlError::UnsupportedValue {
            name: "channel_binding",
            value: other.to_owned(),
        }),
    }
}

fn host_order(value: Option<&str>) -> Result<HostOrder, UrlError> {
    match value {
        None | Some("disable") => Ok(HostOrder::AsGiven),
        Some("random") => Ok(HostOrder::Random),
        Some(other) => Err(UrlError::UnsupportedValue {
            name: "load_balance_hosts",
            value: other.to_owned(),
        }),
    }
}

/// Decodes `%XX` escapes. `+` stays `+`, as in libpq.
fn percent_decode(text: &str, component: &UrlComponent) -> Result<Vec<u8>, UrlError> {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                let high = bytes.get(index + 1).copied().and_then(hex_value);
                let low = bytes.get(index + 2).copied().and_then(hex_value);
                let (Some(high), Some(low)) = (high, low) else {
                    return Err(UrlError::InvalidPercentEncoding {
                        component: component.clone(),
                    });
                };
                decoded.push(high * 16 + low);
                index += 3;
            }
            other => {
                decoded.push(other);
                index += 1;
            }
        }
    }
    Ok(decoded)
}

fn decode_text(text: &str, component: &UrlComponent) -> Result<String, UrlError> {
    String::from_utf8(percent_decode(text, component)?).map_err(|_| UrlError::NotUtf8 {
        component: component.clone(),
    })
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::db::environment::{EnvironmentPassword, HomeDirectory};
    use crate::db::tls::{AttemptOrder, FileRoots, StrictRoots, TlsMode};

    fn environment_with(
        variables: Vec<&'static str>,
        password: EnvironmentPassword,
    ) -> ConnectionEnvironment {
        ConnectionEnvironment::new(password, variables, HomeDirectory::Unknown)
    }

    fn parse(text: &str) -> Result<ConnectionSettings, UrlError> {
        parse_connection_url(text, &ConnectionEnvironment::isolated())
    }

    fn tcp(host: &str, port: u16) -> Endpoint {
        Endpoint {
            host: HostAddress::Tcp(host.to_owned()),
            port,
        }
    }

    #[test]
    fn full_url_parses_every_part() {
        let settings =
            parse("postgresql://alice:s%40cret@db.example.com:6543/jobs?application_name=w1")
                .expect("valid URL");
        assert_eq!(settings.endpoints(), &[tcp("db.example.com", 6543)]);
        assert_eq!(settings.user(), &UserName::Named("alice".to_owned()));
        assert_eq!(
            settings.password(),
            &UrlPassword::Given(DatabasePassword::from_bytes(b"s@cret".to_vec())),
        );
        assert_eq!(settings.database(), &DatabaseName::Named("jobs".to_owned()));
        assert_eq!(settings.application_name(), Some("w1"));
        assert_eq!(settings.connect_timeout(), DEFAULT_CONNECT_TIMEOUT);
        assert_eq!(
            settings.tls().mode(),
            &TlsMode::Weak {
                order: AttemptOrder::TlsThenPlain,
                roots: FileRoots::Default
            },
        );
    }

    #[test]
    fn both_schemes_are_accepted() {
        assert!(parse("postgres://h/d").is_ok());
        assert!(parse("postgresql://h/d").is_ok());
    }

    #[test]
    fn a_driver_suffix_scheme_is_refused_by_name() {
        assert_eq!(
            parse("postgresql+psycopg://h/d"),
            Err(UrlError::UnsupportedScheme {
                scheme: "postgresql+psycopg".to_owned()
            }),
        );
    }

    #[test]
    fn key_value_conninfo_is_not_a_url() {
        assert_eq!(parse("host=h dbname=d"), Err(UrlError::NotAUrl));
    }

    #[test]
    fn a_password_never_appears_in_a_scheme_error() {
        let error = parse("host=h password=se://cret").expect_err("not a URL");
        assert_eq!(error, UrlError::NotAUrl);
    }

    #[test]
    fn missing_parts_take_libpq_defaults() {
        let settings = parse("postgresql://h").expect("valid URL");
        assert_eq!(settings.endpoints(), &[tcp("h", DEFAULT_PORT)]);
        assert_eq!(settings.user(), &UserName::ProcessUser);
        assert_eq!(settings.password(), &UrlPassword::NotGiven);
        assert_eq!(settings.database(), &DatabaseName::SameAsUser);
        assert_eq!(settings.target_session(), TargetSession::Any);
        assert_eq!(settings.channel_binding(), ChannelBindingPolicy::Prefer);
        assert_eq!(settings.host_order(), HostOrder::AsGiven);
    }

    #[test]
    fn an_empty_password_is_not_given() {
        let settings = parse("postgresql://u:@h/d").expect("valid URL");
        assert_eq!(settings.password(), &UrlPassword::NotGiven);
    }

    #[test]
    fn several_hosts_with_their_ports() {
        let settings = parse("postgresql://a:5433,b,[::1]:5435/d").expect("valid URL");
        assert_eq!(
            settings.endpoints(),
            &[tcp("a", 5433), tcp("b", DEFAULT_PORT), tcp("::1", 5435)],
        );
    }

    #[test]
    fn a_unix_socket_directory_from_the_query() {
        let settings =
            parse("postgresql:///d?host=%2Fvar%2Frun%2Fpostgresql&port=5433").expect("valid URL");
        assert_eq!(
            settings.endpoints(),
            &[Endpoint {
                host: HostAddress::UnixSocketDirectory("/var/run/postgresql".to_owned()),
                port: 5433,
            }],
        );
    }

    #[test]
    fn a_unix_socket_directory_in_the_body_is_percent_encoded() {
        let settings = parse("postgresql://%2Ftmp/d").expect("valid URL");
        assert_eq!(
            settings.endpoints()[0].host,
            HostAddress::UnixSocketDirectory("/tmp".to_owned()),
        );
    }

    #[test]
    fn one_port_parameter_applies_to_every_host() {
        let settings = parse("postgresql:///d?host=a,b&port=7000").expect("valid URL");
        assert_eq!(settings.endpoints(), &[tcp("a", 7000), tcp("b", 7000)]);
    }

    #[test]
    fn port_count_must_match_host_count() {
        assert_eq!(
            parse("postgresql:///d?host=a,b,c&port=1,2"),
            Err(UrlError::PortCountMismatch { hosts: 3, ports: 2 }),
        );
    }

    #[test]
    fn a_url_without_a_host_is_refused() {
        assert_eq!(parse("postgresql:///d"), Err(UrlError::HostMissing));
        assert_eq!(parse("postgresql://a,,b/d"), Err(UrlError::HostMissing));
    }

    #[test]
    fn invalid_ports_are_refused() {
        for url in [
            "postgresql://h:0/d",
            "postgresql://h:70000/d",
            "postgresql://h:x/d",
        ] {
            assert!(
                matches!(parse(url), Err(UrlError::InvalidPort { .. })),
                "{url}"
            );
        }
    }

    #[test]
    fn too_many_hosts_are_refused() {
        let hosts = vec!["h"; MAX_HOSTS + 1].join(",");
        assert_eq!(
            parse(&format!("postgresql://{hosts}/d")),
            Err(UrlError::TooManyHosts {
                count: MAX_HOSTS + 1,
                limit: MAX_HOSTS
            }),
        );
    }

    #[test]
    fn an_unclosed_ipv6_bracket_is_refused() {
        assert_eq!(
            parse("postgresql://[::1/d"),
            Err(UrlError::UnclosedIpv6Bracket)
        );
        assert_eq!(
            parse("postgresql://[::1]x/d"),
            Err(UrlError::TextAfterIpv6Host)
        );
    }

    #[test]
    fn keepalive_parameters_are_refused_as_set_by_core() {
        for name in SET_BY_CORE_PARAMETERS {
            assert_eq!(
                parse(&format!("postgresql://h/d?{name}=1")),
                Err(UrlError::UnsupportedParameter {
                    name: (*name).to_owned(),
                    reason: UnsupportedReason::SetByCore,
                }),
            );
        }
    }

    #[test]
    fn unsupported_libpq_parameters_are_refused_by_name() {
        for name in NOT_SUPPORTED_PARAMETERS {
            assert_eq!(
                parse(&format!("postgresql://h/d?{name}=x")),
                Err(UrlError::UnsupportedParameter {
                    name: (*name).to_owned(),
                    reason: UnsupportedReason::NotSupported,
                }),
            );
        }
    }

    #[test]
    fn an_unknown_parameter_is_refused_by_name() {
        assert_eq!(
            parse("postgresql://h/d?statement_cache_size=0"),
            Err(UrlError::UnsupportedParameter {
                name: "statement_cache_size".to_owned(),
                reason: UnsupportedReason::Unknown,
            }),
        );
    }

    #[test]
    fn a_parameter_given_twice_is_refused() {
        assert_eq!(
            parse("postgresql://h/d?sslmode=require&sslmode=disable"),
            Err(duplicate("sslmode")),
        );
        assert_eq!(parse("postgresql://u@h/d?user=v"), Err(duplicate("user")));
        assert_eq!(
            parse("postgresql://u:p@h/d?password=q"),
            Err(duplicate("password"))
        );
        assert_eq!(parse("postgresql://h/d?dbname=e"), Err(duplicate("dbname")));
        assert_eq!(parse("postgresql://h/d?host=g"), Err(duplicate("host")));
        assert_eq!(
            parse("postgresql://h:5433/d?port=5434"),
            Err(duplicate("port"))
        );
    }

    #[test]
    fn a_parameter_without_a_value_separator_is_refused() {
        assert_eq!(
            parse("postgresql://h/d?sslmode"),
            Err(UrlError::MalformedParameter { position: 1 }),
        );
    }

    #[test]
    fn invalid_percent_encoding_names_the_component_only() {
        assert_eq!(
            parse("postgresql://u:p%zz@h/d"),
            Err(UrlError::InvalidPercentEncoding {
                component: UrlComponent::Password
            }),
        );
        assert_eq!(
            parse("postgresql://u@h/d%4"),
            Err(UrlError::InvalidPercentEncoding {
                component: UrlComponent::Database
            }),
        );
    }

    #[test]
    fn the_password_may_be_any_bytes() {
        let settings = parse("postgresql://u:%FF%00@h/d").expect("valid URL");
        assert_eq!(
            settings.password(),
            &UrlPassword::Given(DatabasePassword::from_bytes(vec![0xFF, 0x00])),
        );
    }

    #[test]
    fn the_masked_url_hides_both_password_places() {
        let settings = parse("postgresql://u:secret@h:5432/d?sslmode=disable").expect("valid URL");
        assert_eq!(
            settings.masked_url().as_str(),
            "postgresql://u:***@h:5432/d?sslmode=disable",
        );
        let settings =
            parse("postgres://h/d?user=u&password=secret&application_name=a").expect("valid URL");
        assert_eq!(
            settings.masked_url().as_str(),
            "postgres://h/d?user=u&password=***&application_name=a",
        );
        assert!(!format!("{settings:?}").contains("secret"));
    }

    #[test]
    fn connect_timeout_must_be_bounded() {
        let settings = parse("postgresql://h/d?connect_timeout=3").expect("valid URL");
        assert_eq!(settings.connect_timeout(), Duration::from_secs(3));
        for value in ["0", "-1"] {
            assert!(
                matches!(
                    parse(&format!("postgresql://h/d?connect_timeout={value}")),
                    Err(UrlError::UnboundedConnectTimeout { .. })
                ),
                "{value}"
            );
        }
        assert_eq!(
            parse("postgresql://h/d?connect_timeout=3601"),
            Err(UrlError::ConnectTimeoutTooLong {
                seconds: 3601,
                limit: MAX_CONNECT_TIMEOUT_SECONDS
            }),
        );
        assert!(matches!(
            parse("postgresql://h/d?connect_timeout=soon"),
            Err(UrlError::UnsupportedValue {
                name: "connect_timeout",
                ..
            })
        ));
    }

    #[test]
    fn all_six_sslmode_values_parse() {
        let named = || FileRoots::File(PathBuf::from("/r.crt"));
        for (value, mode) in [
            ("disable", TlsMode::Disabled),
            (
                "allow",
                TlsMode::Weak {
                    order: AttemptOrder::PlainThenTls,
                    roots: FileRoots::Default,
                },
            ),
            (
                "prefer",
                TlsMode::Weak {
                    order: AttemptOrder::TlsThenPlain,
                    roots: FileRoots::Default,
                },
            ),
            (
                "require",
                TlsMode::Weak {
                    order: AttemptOrder::TlsOnly,
                    roots: FileRoots::Default,
                },
            ),
            (
                "verify-ca&sslrootcert=%2Fr.crt",
                TlsMode::VerifyChain { roots: named() },
            ),
            (
                "verify-full&sslrootcert=%2Fr.crt",
                TlsMode::VerifyChainAndHostName {
                    roots: StrictRoots::Files(named()),
                },
            ),
            (
                "verify-full",
                TlsMode::VerifyChainAndHostName {
                    roots: StrictRoots::Files(FileRoots::Default),
                },
            ),
        ] {
            let settings = parse(&format!("postgresql://h/d?sslmode={value}")).expect("valid URL");
            assert_eq!(settings.tls().mode(), &mode, "{value}");
        }
    }

    #[test]
    fn an_unknown_sslmode_is_refused() {
        assert_eq!(
            parse("postgresql://h/d?sslmode=verify_full"),
            Err(UrlError::UnsupportedValue {
                name: "sslmode",
                value: "verify_full".to_owned()
            }),
        );
    }

    #[test]
    fn tls_policy_errors_surface_from_the_parser() {
        assert_eq!(
            parse("postgresql://h/d?sslmode=require&sslrootcert=system"),
            Err(UrlError::Tls(TlsPolicyError::WeakSslModeWithSystemRoots {
                sslmode: SslMode::Require
            })),
        );
        assert_eq!(
            parse("postgresql://h/d?sslnegotiation=direct"),
            Err(UrlError::Tls(
                TlsPolicyError::DirectNegotiationNeedsRequiredTls {
                    sslmode: SslMode::Prefer
                }
            )),
        );
    }

    #[test]
    fn client_certificate_and_key_parse_one_by_one() {
        let settings = parse("postgresql://h/d?sslmode=require&sslcert=%2Fc.crt&sslkey=%2Fc.key")
            .expect("valid URL");
        assert_eq!(
            settings.tls().certificate(),
            &ClientFile::File(PathBuf::from("/c.crt"))
        );
        assert_eq!(
            settings.tls().key(),
            &ClientFile::File(PathBuf::from("/c.key"))
        );
        let only_certificate = parse("postgresql://h/d?sslcert=%2Fc.crt").expect("valid URL");
        assert_eq!(only_certificate.tls().key(), &ClientFile::Default);
    }

    #[test]
    fn an_empty_query_password_skips_pgpassword() {
        let settings = parse("postgresql://u@h/d?password=").expect("valid URL");
        assert_eq!(settings.password(), &UrlPassword::GivenEmpty);
    }

    #[test]
    fn target_session_values() {
        let settings =
            parse("postgresql://h/d?target_session_attrs=read-write").expect("valid URL");
        assert_eq!(settings.target_session(), TargetSession::ReadWrite);
        assert_eq!(
            parse("postgresql://h/d?target_session_attrs=primary"),
            Err(UrlError::UnsupportedValue {
                name: "target_session_attrs",
                value: "primary".to_owned()
            }),
        );
    }

    #[test]
    fn a_variable_is_refused_when_the_url_does_not_set_its_parameter() {
        let environment = environment_with(vec!["PGSSLMODE"], EnvironmentPassword::NotSet);
        assert_eq!(
            parse_connection_url("postgresql://h/d", &environment),
            Err(UrlError::EnvironmentDefault {
                name: "PGSSLMODE",
                parameter: "sslmode"
            }),
        );
    }

    #[test]
    fn a_variable_is_ignored_when_the_url_sets_its_parameter() {
        let environment = environment_with(
            vec![
                "PGHOST",
                "PGPORT",
                "PGUSER",
                "PGDATABASE",
                "PGSSLMODE",
                "PGREQUIRESSL",
            ],
            EnvironmentPassword::NotSet,
        );
        assert!(
            parse_connection_url("postgresql://u@h:5433/d?sslmode=require", &environment).is_ok()
        );
        let empty_query_values =
            environment_with(vec!["PGUSER", "PGDATABASE"], EnvironmentPassword::NotSet);
        assert!(parse_connection_url("postgresql://h?user=&dbname=", &empty_query_values).is_ok());
    }

    #[test]
    fn an_empty_body_part_does_not_set_its_parameter() {
        let environment = environment_with(vec!["PGUSER"], EnvironmentPassword::NotSet);
        assert_eq!(
            parse_connection_url("postgresql://@h/d", &environment),
            Err(UrlError::EnvironmentDefault {
                name: "PGUSER",
                parameter: "user"
            }),
        );
        let host_variable = environment_with(vec!["PGHOST"], EnvironmentPassword::NotSet);
        assert!(matches!(
            parse_connection_url("postgresql://:5432/d", &host_variable),
            Err(UrlError::HostMissing)
        ));
    }

    #[test]
    fn an_unsupported_variable_is_refused_whenever_it_is_set() {
        for name in ["PGSERVICE", "PGTZ", "PGGSSENCMODE", "PGSSLCRL"] {
            let environment = environment_with(vec![name], EnvironmentPassword::NotSet);
            assert_eq!(
                parse_connection_url("postgresql://u:p@h/d?sslmode=require", &environment),
                Err(UrlError::EnvironmentNotSupported { name }),
            );
        }
    }

    #[test]
    fn a_variable_without_connection_effect_is_ignored() {
        let environment = environment_with(
            vec!["PGSERVICEFILE", "PGSYSCONFDIR", "PGLOCALEDIR"],
            EnvironmentPassword::NotSet,
        );
        assert!(parse_connection_url("postgresql://h/d", &environment).is_ok());
    }

    #[test]
    fn pgpassfile_is_refused_only_when_the_password_file_is_read() {
        let unset = environment_with(vec!["PGPASSFILE"], EnvironmentPassword::NotSet);
        assert_eq!(
            parse_connection_url("postgresql://u@h/d", &unset),
            Err(UrlError::EnvironmentPasswordFile { name: "PGPASSFILE" }),
        );
        assert_eq!(
            parse_connection_url("postgresql://u@h/d?password=", &unset),
            Err(UrlError::EnvironmentPasswordFile { name: "PGPASSFILE" }),
        );
        assert!(parse_connection_url("postgresql://u:p@h/d", &unset).is_ok());
        let with_pgpassword = environment_with(
            vec!["PGPASSFILE"],
            EnvironmentPassword::Set(DatabasePassword::from_bytes(b"p".to_vec())),
        );
        assert!(parse_connection_url("postgresql://u@h/d", &with_pgpassword).is_ok());
    }

    #[test]
    fn a_url_over_the_limit_is_refused() {
        let text = format!("postgresql://h/{}", "d".repeat(MAX_URL_BYTES));
        assert!(matches!(parse(&text), Err(UrlError::TooLong { .. })));
    }

    #[test]
    fn too_many_query_parameters_are_refused() {
        let query = vec!["application_name=a"; MAX_QUERY_PARAMETERS + 1].join("&");
        assert_eq!(
            parse(&format!("postgresql://h/d?{query}")),
            Err(UrlError::TooManyParameters {
                count: MAX_QUERY_PARAMETERS + 1,
                limit: MAX_QUERY_PARAMETERS
            }),
        );
    }
}

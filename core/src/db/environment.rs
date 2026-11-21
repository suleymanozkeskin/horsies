//! What the connection layer takes from outside the URL: `PGPASSWORD`, the
//! home directory (for `~/.pgpass` and the default TLS files under
//! `~/.postgresql/`), and which other libpq environment variables are set.
//!
//! libpq applies most `PG*` variables only as defaults for a parameter that
//! the connection string does not set. The parser follows that rule: a set
//! variable whose parameter the URL sets is ignored, as libpq ignores it; a
//! set variable that would apply is refused by name ([`VariableRule`]).
//!
//! Password order, as libpq: the URL, then `PGPASSWORD`, then `~/.pgpass`.

use std::io::Read;
use std::path::{Path, PathBuf};

use super::url::DatabasePassword;

/// The one libpq environment variable the core reads.
pub const PASSWORD_VARIABLE: &str = "PGPASSWORD";

/// What a set libpq environment variable does under libpq.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VariableRule {
    /// The default of this URL parameter: it applies only when the URL does
    /// not set the parameter.
    DefaultFor(&'static str),
    /// It applies whenever it is set, and horsies does not support it.
    AlwaysApplies,
    /// `PGPASSFILE`: it applies when the password comes from the password
    /// file, that is when neither the URL nor `PGPASSWORD` gives one.
    PasswordFileLocation,
    /// It changes nothing about the connection: the service-file location
    /// without a service, and the message translations of libpq.
    NoConnectionEffect,
}

/// Every libpq environment variable other than `PGPASSWORD`, with its rule.
pub const LIBPQ_VARIABLES: &[(&str, VariableRule)] = &[
    ("PGHOST", VariableRule::DefaultFor("host")),
    ("PGHOSTADDR", VariableRule::AlwaysApplies),
    ("PGPORT", VariableRule::DefaultFor("port")),
    ("PGDATABASE", VariableRule::DefaultFor("dbname")),
    ("PGUSER", VariableRule::DefaultFor("user")),
    ("PGPASSFILE", VariableRule::PasswordFileLocation),
    ("PGREQUIREAUTH", VariableRule::AlwaysApplies),
    (
        "PGCHANNELBINDING",
        VariableRule::DefaultFor("channel_binding"),
    ),
    ("PGSERVICE", VariableRule::AlwaysApplies),
    ("PGSERVICEFILE", VariableRule::NoConnectionEffect),
    ("PGOPTIONS", VariableRule::DefaultFor("options")),
    ("PGAPPNAME", VariableRule::DefaultFor("application_name")),
    ("PGSSLMODE", VariableRule::DefaultFor("sslmode")),
    ("PGREQUIRESSL", VariableRule::DefaultFor("sslmode")),
    (
        "PGSSLNEGOTIATION",
        VariableRule::DefaultFor("sslnegotiation"),
    ),
    ("PGSSLCOMPRESSION", VariableRule::AlwaysApplies),
    ("PGSSLCERT", VariableRule::DefaultFor("sslcert")),
    ("PGSSLKEY", VariableRule::DefaultFor("sslkey")),
    ("PGSSLCERTMODE", VariableRule::AlwaysApplies),
    ("PGSSLROOTCERT", VariableRule::DefaultFor("sslrootcert")),
    ("PGSSLCRL", VariableRule::AlwaysApplies),
    ("PGSSLCRLDIR", VariableRule::AlwaysApplies),
    ("PGSSLSNI", VariableRule::AlwaysApplies),
    ("PGREQUIREPEER", VariableRule::AlwaysApplies),
    ("PGSSLMINPROTOCOLVERSION", VariableRule::AlwaysApplies),
    ("PGSSLMAXPROTOCOLVERSION", VariableRule::AlwaysApplies),
    ("PGGSSENCMODE", VariableRule::AlwaysApplies),
    ("PGKRBSRVNAME", VariableRule::AlwaysApplies),
    ("PGGSSLIB", VariableRule::AlwaysApplies),
    ("PGGSSDELEGATION", VariableRule::AlwaysApplies),
    (
        "PGCONNECT_TIMEOUT",
        VariableRule::DefaultFor("connect_timeout"),
    ),
    ("PGCLIENTENCODING", VariableRule::AlwaysApplies),
    (
        "PGTARGETSESSIONATTRS",
        VariableRule::DefaultFor("target_session_attrs"),
    ),
    (
        "PGLOADBALANCEHOSTS",
        VariableRule::DefaultFor("load_balance_hosts"),
    ),
    ("PGMINPROTOCOLVERSION", VariableRule::AlwaysApplies),
    ("PGMAXPROTOCOLVERSION", VariableRule::AlwaysApplies),
    ("PGDATESTYLE", VariableRule::AlwaysApplies),
    ("PGTZ", VariableRule::AlwaysApplies),
    ("PGGEQO", VariableRule::AlwaysApplies),
    ("PGSYSCONFDIR", VariableRule::NoConnectionEffect),
    ("PGLOCALEDIR", VariableRule::NoConnectionEffect),
];

/// The rule of a libpq variable. A name outside [`LIBPQ_VARIABLES`] is
/// treated as a variable that always applies.
pub fn variable_rule(name: &str) -> VariableRule {
    LIBPQ_VARIABLES
        .iter()
        .find(|(listed, _)| *listed == name)
        .map_or(VariableRule::AlwaysApplies, |(_, rule)| *rule)
}

/// The password file name under the home directory.
pub const PASSWORD_FILE_NAME: &str = ".pgpass";

/// The largest password file the core reads.
pub const MAX_PASSWORD_FILE_BYTES: u64 = 1024 * 1024;

/// The most lines the core reads from the password file.
pub const MAX_PASSWORD_FILE_LINES: usize = 10_000;

/// The `PGPASSWORD` variable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvironmentPassword {
    Set(DatabasePassword),
    /// Unset, or set to the empty text (libpq treats both the same).
    NotSet,
}

/// The home directory, where `.pgpass` and `.postgresql/` live.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HomeDirectory {
    Known(PathBuf),
    /// `HOME` is unset or empty: no file under the home directory is read.
    Unknown,
}

/// The environment, read once, as a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionEnvironment {
    password: EnvironmentPassword,
    set_libpq_variables: Vec<&'static str>,
    home: HomeDirectory,
}

/// Reading the process environment failed.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum EnvironmentError {
    #[error("the environment variable {name} is not valid Unicode")]
    NotUnicode { name: &'static str },
}

impl ConnectionEnvironment {
    /// Reads `PGPASSWORD`, `HOME` and which of [`LIBPQ_VARIABLES`] are set.
    pub fn capture() -> Result<ConnectionEnvironment, EnvironmentError> {
        let password = match std::env::var(PASSWORD_VARIABLE) {
            Ok(text) if text.is_empty() => EnvironmentPassword::NotSet,
            Ok(text) => EnvironmentPassword::Set(DatabasePassword::from_bytes(text.into_bytes())),
            Err(std::env::VarError::NotPresent) => EnvironmentPassword::NotSet,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(EnvironmentError::NotUnicode {
                    name: PASSWORD_VARIABLE,
                })
            }
        };
        let set_libpq_variables = LIBPQ_VARIABLES
            .iter()
            .map(|(name, _)| *name)
            .filter(|name| std::env::var_os(name).is_some())
            .collect();
        let home = match std::env::var_os("HOME") {
            Some(path) if !path.is_empty() => HomeDirectory::Known(PathBuf::from(path)),
            Some(_) | None => HomeDirectory::Unknown,
        };
        Ok(ConnectionEnvironment {
            password,
            set_libpq_variables,
            home,
        })
    }

    /// An environment with nothing in it: no `PGPASSWORD`, no libpq variable,
    /// no home directory. Tests and hosts that pass every setting in the URL
    /// use it.
    pub fn isolated() -> ConnectionEnvironment {
        ConnectionEnvironment {
            password: EnvironmentPassword::NotSet,
            set_libpq_variables: Vec::new(),
            home: HomeDirectory::Unknown,
        }
    }

    /// An environment from explicit parts.
    pub fn new(
        password: EnvironmentPassword,
        set_libpq_variables: Vec<&'static str>,
        home: HomeDirectory,
    ) -> ConnectionEnvironment {
        ConnectionEnvironment {
            password,
            set_libpq_variables,
            home,
        }
    }

    pub fn password(&self) -> &EnvironmentPassword {
        &self.password
    }

    pub fn home(&self) -> &HomeDirectory {
        &self.home
    }

    /// The set libpq variables other than `PGPASSWORD`, in table order.
    pub fn set_libpq_variables(&self) -> &[&'static str] {
        &self.set_libpq_variables
    }
}

/// One field of a password-file line: `*` matches every value.
#[derive(Clone, Debug, PartialEq, Eq)]
enum FieldPattern {
    Any,
    Exact(String),
}

impl FieldPattern {
    fn from_raw(raw: &str) -> FieldPattern {
        match raw {
            "*" => FieldPattern::Any,
            _ => FieldPattern::Exact(unescape(raw)),
        }
    }

    fn matches(&self, value: &str) -> bool {
        match self {
            FieldPattern::Any => true,
            FieldPattern::Exact(text) => text == value,
        }
    }
}

/// One line of a password file: `host:port:database:user:password`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PasswordFileEntry {
    host: FieldPattern,
    port: FieldPattern,
    database: FieldPattern,
    user: FieldPattern,
    password: DatabasePassword,
}

/// The connection a password is looked up for.
pub struct PasswordLookup<'a> {
    pub host: &'a str,
    pub port: u16,
    pub database: &'a str,
    pub user: &'a str,
}

/// The result of a password lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PasswordFileMatch {
    Found(DatabasePassword),
    NoMatch,
}

/// The password file as read from disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PasswordFileRead {
    /// No home directory, or no `.pgpass` in it.
    Absent,
    Contents {
        path: PathBuf,
        text: String,
    },
}

/// The password file exists and cannot be used.
#[derive(Debug, thiserror::Error)]
pub enum PasswordFileError {
    #[error("reading the password file {} failed: {cause}", .path.display())]
    ReadFailed {
        path: PathBuf,
        cause: std::io::Error,
    },
    #[error("the password file {} is not a regular file", .path.display())]
    NotRegularFile { path: PathBuf },
    #[error(
        "the password file {} has group or world access (mode {mode:o}); use u=rw (0600) or less",
        .path.display()
    )]
    TooOpen { path: PathBuf, mode: u32 },
    #[error("the password file {} is larger than {limit} bytes", .path.display())]
    TooLarge { path: PathBuf, limit: u64 },
    #[error("the password file {} is not valid UTF-8", .path.display())]
    NotUtf8 { path: PathBuf },
    #[error("the password file has more than {limit} lines")]
    TooManyLines { limit: usize },
}

/// Parses the text of a password file. Empty lines and lines that start with
/// `#` are skipped; a line with fewer than five fields is skipped, as in
/// libpq. `\` escapes the next character. The password ends at the first
/// unescaped `:` or at the end of the line.
pub fn parse_password_file(text: &str) -> Result<Vec<PasswordFileEntry>, PasswordFileError> {
    let mut entries = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if index >= MAX_PASSWORD_FILE_LINES {
            return Err(PasswordFileError::TooManyLines {
                limit: MAX_PASSWORD_FILE_LINES,
            });
        }
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(entry) = parse_line(line) {
            entries.push(entry);
        }
    }
    Ok(entries)
}

fn parse_line(line: &str) -> Option<PasswordFileEntry> {
    let fields = split_raw_fields(line);
    let [host, port, database, user, password] = fields.as_slice() else {
        return None;
    };
    Some(PasswordFileEntry {
        host: FieldPattern::from_raw(host),
        port: FieldPattern::from_raw(port),
        database: FieldPattern::from_raw(database),
        user: FieldPattern::from_raw(user),
        password: DatabasePassword::from_bytes(unescape(password).into_bytes()),
    })
}

/// Splits a line at unescaped `:` into at most five raw fields (escapes kept).
fn split_raw_fields(line: &str) -> Vec<&str> {
    const FIELD_COUNT: usize = 5;
    let mut fields = Vec::with_capacity(FIELD_COUNT);
    let mut field_start = 0;
    let mut escaped = false;
    for (position, character) in line.char_indices() {
        match (escaped, character) {
            (true, _) => escaped = false,
            (false, '\\') => escaped = true,
            (false, ':') if fields.len() + 1 < FIELD_COUNT => {
                fields.push(&line[field_start..position]);
                field_start = position + 1;
            }
            (false, ':') => {
                fields.push(&line[field_start..position]);
                return fields;
            }
            (false, _) => {}
        }
    }
    fields.push(&line[field_start..]);
    fields
}

/// Removes `\` escapes: `\x` becomes `x`; a trailing `\` stays.
fn unescape(raw: &str) -> String {
    let mut text = String::with_capacity(raw.len());
    let mut characters = raw.chars();
    while let Some(character) = characters.next() {
        match (character, character == '\\') {
            (_, true) => match characters.next() {
                Some(next) => text.push(next),
                None => text.push('\\'),
            },
            (other, false) => text.push(other),
        }
    }
    text
}

/// The password of the first entry that matches all four fields.
pub fn lookup_password(
    entries: &[PasswordFileEntry],
    lookup: &PasswordLookup<'_>,
) -> PasswordFileMatch {
    let port = lookup.port.to_string();
    let found = entries.iter().find(|entry| {
        entry.host.matches(lookup.host)
            && entry.port.matches(&port)
            && entry.database.matches(lookup.database)
            && entry.user.matches(lookup.user)
    });
    match found {
        Some(entry) => PasswordFileMatch::Found(entry.password.clone()),
        None => PasswordFileMatch::NoMatch,
    }
}

/// Reads `~/.pgpass` (blocking file I/O).
///
/// Outcomes: [`PasswordFileRead::Absent`] when the home directory is unknown
/// or the file does not exist; the text otherwise. A file that is not a
/// regular file, has group or world access (Unix), is larger than
/// [`MAX_PASSWORD_FILE_BYTES`] or is not UTF-8 is refused. libpq ignores such
/// a file with a warning; the core refuses it, so authentication does not
/// fail later with a less clear error.
pub fn read_password_file(home: &HomeDirectory) -> Result<PasswordFileRead, PasswordFileError> {
    let HomeDirectory::Known(directory) = home else {
        return Ok(PasswordFileRead::Absent);
    };
    let path = directory.join(PASSWORD_FILE_NAME);
    let metadata = match std::fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PasswordFileRead::Absent)
        }
        Err(cause) => return Err(PasswordFileError::ReadFailed { path, cause }),
    };
    if !metadata.is_file() {
        return Err(PasswordFileError::NotRegularFile { path });
    }
    check_permissions(&path, &metadata)?;
    let bytes = read_bounded(&path)?;
    match String::from_utf8(bytes) {
        Ok(text) => Ok(PasswordFileRead::Contents { path, text }),
        Err(_) => Err(PasswordFileError::NotUtf8 { path }),
    }
}

#[cfg(unix)]
fn check_permissions(path: &Path, metadata: &std::fs::Metadata) -> Result<(), PasswordFileError> {
    use std::os::unix::fs::PermissionsExt;

    let mode = metadata.permissions().mode() & 0o777;
    match mode & 0o077 {
        0 => Ok(()),
        _ => Err(PasswordFileError::TooOpen {
            path: path.to_path_buf(),
            mode,
        }),
    }
}

/// Platforms without Unix modes have no permission rule in libpq either.
#[cfg(not(unix))]
fn check_permissions(_path: &Path, _metadata: &std::fs::Metadata) -> Result<(), PasswordFileError> {
    Ok(())
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, PasswordFileError> {
    let read_failed = |cause: std::io::Error| PasswordFileError::ReadFailed {
        path: path.to_path_buf(),
        cause,
    };
    let handle = std::fs::File::open(path).map_err(read_failed)?;
    let mut bytes = Vec::new();
    let _length = handle
        .take(MAX_PASSWORD_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(read_failed)?;
    match u64::try_from(bytes.len()) {
        Ok(length) if length <= MAX_PASSWORD_FILE_BYTES => Ok(bytes),
        Ok(_) | Err(_) => Err(PasswordFileError::TooLarge {
            path: path.to_path_buf(),
            limit: MAX_PASSWORD_FILE_BYTES,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup<'a>(
        host: &'a str,
        port: u16,
        database: &'a str,
        user: &'a str,
    ) -> PasswordLookup<'a> {
        PasswordLookup {
            host,
            port,
            database,
            user,
        }
    }

    fn found(text: &str) -> PasswordFileMatch {
        PasswordFileMatch::Found(DatabasePassword::from_bytes(text.as_bytes().to_vec()))
    }

    #[test]
    fn exact_and_wildcard_fields_match() {
        let entries = parse_password_file(
            "# comment\n\
             db.example.com:5432:jobs:alice:first\n\
             *:*:*:bob:second\n",
        )
        .expect("valid file");
        assert_eq!(
            lookup_password(&entries, &lookup("db.example.com", 5432, "jobs", "alice")),
            found("first"),
        );
        assert_eq!(
            lookup_password(&entries, &lookup("other", 6432, "x", "bob")),
            found("second"),
        );
        assert_eq!(
            lookup_password(&entries, &lookup("db.example.com", 5433, "jobs", "alice")),
            PasswordFileMatch::NoMatch,
        );
    }

    #[test]
    fn the_first_matching_line_wins() {
        let entries = parse_password_file("*:*:*:*:one\n*:*:*:*:two\n").expect("valid file");
        assert_eq!(
            lookup_password(&entries, &lookup("h", 5432, "d", "u")),
            found("one")
        );
    }

    #[test]
    fn escapes_keep_colons_and_backslashes() {
        let entries = parse_password_file("h\\:x:5432:d:u:pa\\:ss\\\\word\n").expect("valid file");
        assert_eq!(
            lookup_password(&entries, &lookup("h:x", 5432, "d", "u")),
            found("pa:ss\\word"),
        );
    }

    #[test]
    fn an_escaped_star_is_a_literal_star() {
        let entries = parse_password_file("\\*:5432:d:u:p\n").expect("valid file");
        assert_eq!(
            lookup_password(&entries, &lookup("h", 5432, "d", "u")),
            PasswordFileMatch::NoMatch,
        );
        assert_eq!(
            lookup_password(&entries, &lookup("*", 5432, "d", "u")),
            found("p")
        );
    }

    #[test]
    fn the_password_ends_at_an_unescaped_colon() {
        let entries = parse_password_file("h:5432:d:u:pass:rest\n").expect("valid file");
        assert_eq!(
            lookup_password(&entries, &lookup("h", 5432, "d", "u")),
            found("pass")
        );
    }

    #[test]
    fn a_short_line_is_skipped() {
        let entries = parse_password_file("h:5432:d:u\n").expect("valid file");
        assert!(entries.is_empty());
    }

    #[test]
    fn too_many_lines_are_refused() {
        let text = "#\n".repeat(MAX_PASSWORD_FILE_LINES + 1);
        assert!(matches!(
            parse_password_file(&text),
            Err(PasswordFileError::TooManyLines { .. })
        ));
    }

    #[test]
    fn an_unknown_home_reads_no_file() {
        assert_eq!(
            read_password_file(&HomeDirectory::Unknown).expect("no read"),
            PasswordFileRead::Absent,
        );
    }

    #[test]
    fn a_missing_file_is_absent() {
        let home = HomeDirectory::Known(
            std::env::temp_dir().join(format!("stmt_home_{}", uuid::Uuid::new_v4())),
        );
        assert_eq!(
            read_password_file(&home).expect("absent is not an error"),
            PasswordFileRead::Absent,
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_group_readable_file_is_refused() {
        use std::os::unix::fs::PermissionsExt;

        let directory = std::env::temp_dir().join(format!("stmt_home_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).expect("create temp home");
        let path = directory.join(PASSWORD_FILE_NAME);
        std::fs::write(&path, "*:*:*:*:p\n").expect("write password file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).expect("set mode");
        let outcome = read_password_file(&HomeDirectory::Known(directory.clone()));
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("set mode");
        let accepted = read_password_file(&HomeDirectory::Known(directory.clone()));
        let _removed = std::fs::remove_dir_all(&directory);
        assert!(matches!(
            outcome,
            Err(PasswordFileError::TooOpen { mode: 0o640, .. })
        ));
        assert!(matches!(accepted, Ok(PasswordFileRead::Contents { .. })));
    }

    #[test]
    fn an_isolated_environment_has_nothing() {
        let environment = ConnectionEnvironment::isolated();
        assert_eq!(environment.password(), &EnvironmentPassword::NotSet);
        assert_eq!(environment.home(), &HomeDirectory::Unknown);
        assert!(environment.set_libpq_variables().is_empty());
    }

    #[test]
    fn each_variable_has_its_libpq_rule() {
        assert_eq!(variable_rule("PGUSER"), VariableRule::DefaultFor("user"));
        assert_eq!(
            variable_rule("PGREQUIRESSL"),
            VariableRule::DefaultFor("sslmode")
        );
        assert_eq!(
            variable_rule("PGPASSFILE"),
            VariableRule::PasswordFileLocation
        );
        assert_eq!(variable_rule("PGSERVICE"), VariableRule::AlwaysApplies);
        assert_eq!(variable_rule("PGTZ"), VariableRule::AlwaysApplies);
        assert_eq!(
            variable_rule("PGSERVICEFILE"),
            VariableRule::NoConnectionEffect
        );
        assert_eq!(variable_rule("PGNOTAVARIABLE"), VariableRule::AlwaysApplies);
        assert!(LIBPQ_VARIABLES
            .iter()
            .all(|(name, _)| *name != PASSWORD_VARIABLE));
    }
}

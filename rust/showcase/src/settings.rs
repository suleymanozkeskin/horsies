//! Database URL resolution for the Acme demo.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use thiserror::Error;
use url::Url;

pub const ACME_URL_VARIABLE: &str = "ACME_DATABASE_URL";
pub const SHARED_URL_VARIABLE: &str = "DATABASE_URL";
pub const ACME_DATABASE_NAME: &str = "acme_demo";
pub const MAINTENANCE_DATABASE_NAME: &str = "postgres";
/// Python's SQLAlchemy/psycopg URL form. Accepted for `.env` compatibility.
pub const SQLALCHEMY_SCHEME: &str = "postgresql+psycopg";
/// Native libpq and SQLx URL forms accepted by the Rust showcase.
pub const POSTGRESQL_SCHEME: &str = "postgresql";
pub const POSTGRES_SCHEME: &str = "postgres";
/// Canonical scheme used for SQLx connections after normalization.
pub const PSYCOPG_SCHEME: &str = "postgresql";
pub const DEFAULT_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/acme_demo";

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct SettingsError(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseSettings {
    /// The URL selected by the five resolution rules.
    pub url: String,
    /// The same target in a libpq/SQLx-compatible form.
    pub psycopg_dsn: String,
    /// The server's maintenance database on the same host and credentials.
    pub maintenance_dsn: String,
    pub database_name: String,
    /// The matching rule. This is printed by `acme seed`.
    pub source: String,
}

impl DatabaseSettings {
    pub fn sqlx_url(&self) -> &str {
        &self.psycopg_dsn
    }
}

fn repository_root(start: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .find(|candidate| candidate.join("pyproject.toml").is_file())
        .map(Path::to_path_buf)
}

fn read_env_file(path: &Path) -> Result<BTreeMap<String, String>, SettingsError> {
    let content = fs::read_to_string(path)
        .map_err(|error| SettingsError(format!("cannot read {}: {error}", path.display())))?;
    let mut values = BTreeMap::new();
    for raw in content.lines() {
        let line = raw.trim().strip_prefix("export ").unwrap_or(raw.trim());
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim().trim_matches(['"', '\'']);
        values.insert(key.trim().to_owned(), value.to_owned());
    }
    Ok(values)
}

fn env_file_values() -> Result<BTreeMap<String, String>, SettingsError> {
    let start = Path::new(env!("CARGO_MANIFEST_DIR"));
    let Some(root) = repository_root(start) else {
        return Ok(BTreeMap::new());
    };
    let path = root.join(".env");
    if !path.is_file() {
        return Ok(BTreeMap::new());
    }
    read_env_file(&path)
}

fn replace_database_name(input: &str, database_name: &str) -> Result<String, SettingsError> {
    let mut parsed = Url::parse(input)
        .map_err(|error| SettingsError(format!("invalid database URL {input:?}: {error}")))?;
    parsed.set_path(&format!("/{database_name}"));
    Ok(parsed.to_string())
}

fn replace_scheme(input: &str, scheme: &str) -> Result<String, SettingsError> {
    let mut parsed = Url::parse(input)
        .map_err(|error| SettingsError(format!("invalid database URL {input:?}: {error}")))?;
    parsed
        .set_scheme(scheme)
        .map_err(|()| SettingsError(format!("cannot replace URL scheme in {input:?}")))?;
    Ok(parsed.to_string())
}

fn validate_url(url: &str, source: &str) -> Result<String, SettingsError> {
    let parsed = Url::parse(url)
        .map_err(|error| SettingsError(format!("{source} is not a valid URL: {error}")))?;
    if !matches!(
        parsed.scheme(),
        SQLALCHEMY_SCHEME | POSTGRESQL_SCHEME | POSTGRES_SCHEME
    ) {
        return Err(SettingsError(format!(
            "{source} must use one of {POSTGRESQL_SCHEME}://, {POSTGRES_SCHEME}://, or {SQLALCHEMY_SCHEME}:// — got {url:?}"
        )));
    }
    let database_name = parsed.path().trim_start_matches('/');
    if database_name.is_empty() {
        return Err(SettingsError(format!(
            "{source} has no database name; append /{ACME_DATABASE_NAME}"
        )));
    }
    Ok(database_name.to_owned())
}

fn select_url(
    env_values: &BTreeMap<String, String>,
    file_values: &BTreeMap<String, String>,
) -> Result<(String, String), SettingsError> {
    if let Some(value) = env_values
        .get(ACME_URL_VARIABLE)
        .filter(|value| !value.trim().is_empty())
    {
        return Ok((
            value.trim().to_owned(),
            format!("environment {ACME_URL_VARIABLE}"),
        ));
    }
    if let Some(value) = file_values
        .get(ACME_URL_VARIABLE)
        .filter(|value| !value.trim().is_empty())
    {
        return Ok((value.trim().to_owned(), format!(".env {ACME_URL_VARIABLE}")));
    }
    if let Some(value) = env_values
        .get(SHARED_URL_VARIABLE)
        .filter(|value| !value.trim().is_empty())
    {
        return Ok((
            replace_database_name(value.trim(), ACME_DATABASE_NAME)?,
            format!("environment {SHARED_URL_VARIABLE} (database -> {ACME_DATABASE_NAME})"),
        ));
    }
    if let Some(value) = file_values
        .get(SHARED_URL_VARIABLE)
        .filter(|value| !value.trim().is_empty())
    {
        return Ok((
            replace_database_name(value.trim(), ACME_DATABASE_NAME)?,
            format!(".env {SHARED_URL_VARIABLE} (database -> {ACME_DATABASE_NAME})"),
        ));
    }
    Ok((
        DEFAULT_DATABASE_URL.to_owned(),
        "built-in default".to_owned(),
    ))
}

pub fn resolve_database_settings() -> Result<DatabaseSettings, SettingsError> {
    let environment = env::vars().collect::<BTreeMap<_, _>>();
    let file = env_file_values()?;
    resolve_database_settings_from(&environment, &file)
}

pub fn resolve_database_settings_from(
    environment: &BTreeMap<String, String>,
    file: &BTreeMap<String, String>,
) -> Result<DatabaseSettings, SettingsError> {
    let (url, source) = select_url(environment, file)?;
    let database_name = validate_url(&url, &source)?;
    let psycopg_dsn = replace_scheme(&url, PSYCOPG_SCHEME)?;
    let maintenance_dsn = replace_database_name(&psycopg_dsn, MAINTENANCE_DATABASE_NAME)?;
    Ok(DatabaseSettings {
        url,
        psycopg_dsn,
        maintenance_dsn,
        database_name,
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn maps(values: &[(&str, &str)]) -> BTreeMap<String, String> {
        values
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect()
    }

    #[test]
    fn resolution_rules_have_the_pinned_precedence() {
        let direct = maps(&[(ACME_URL_VARIABLE, "postgresql://a:b@h:9/direct")]);
        let file_direct = maps(&[(ACME_URL_VARIABLE, "postgresql://a:b@h:9/file")]);
        let shared = maps(&[(
            SHARED_URL_VARIABLE,
            "postgresql://a:b@h:9/horsies?sslmode=require",
        )]);
        let file_shared = maps(&[(SHARED_URL_VARIABLE, "postgresql://a:b@h:9/other")]);
        assert_eq!(
            resolve_database_settings_from(&direct, &file_direct)
                .unwrap()
                .database_name,
            "direct"
        );
        assert_eq!(
            resolve_database_settings_from(&BTreeMap::new(), &file_direct)
                .unwrap()
                .database_name,
            "file"
        );
        let settings = resolve_database_settings_from(&shared, &file_shared).unwrap();
        assert_eq!(settings.database_name, ACME_DATABASE_NAME);
        assert!(settings.url.contains("sslmode=require"));
        assert_eq!(
            settings.source,
            "environment DATABASE_URL (database -> acme_demo)"
        );
        let settings = resolve_database_settings_from(&BTreeMap::new(), &file_shared).unwrap();
        assert_eq!(settings.database_name, ACME_DATABASE_NAME);
        assert_eq!(settings.source, ".env DATABASE_URL (database -> acme_demo)");
        let settings = resolve_database_settings_from(&BTreeMap::new(), &BTreeMap::new()).unwrap();
        assert_eq!(settings.database_name, ACME_DATABASE_NAME);
    }

    #[test]
    fn every_resolution_rule_accepts_native_and_python_url_forms() {
        let accepted = [POSTGRESQL_SCHEME, POSTGRES_SCHEME, SQLALCHEMY_SCHEME];
        for scheme in accepted {
            let direct_env = maps(&[(ACME_URL_VARIABLE, &format!("{scheme}://a:b@h:9/direct"))]);
            assert_eq!(
                resolve_database_settings_from(&direct_env, &BTreeMap::new())
                    .unwrap()
                    .database_name,
                "direct"
            );

            let direct_file = maps(&[(ACME_URL_VARIABLE, &format!("{scheme}://a:b@h:9/file"))]);
            assert_eq!(
                resolve_database_settings_from(&BTreeMap::new(), &direct_file)
                    .unwrap()
                    .database_name,
                "file"
            );

            let shared_env = maps(&[(
                SHARED_URL_VARIABLE,
                &format!("{scheme}://a:b@h:9/shared?sslmode=require"),
            )]);
            let shared = resolve_database_settings_from(&shared_env, &BTreeMap::new()).unwrap();
            assert_eq!(shared.database_name, ACME_DATABASE_NAME);
            assert_eq!(
                shared.psycopg_dsn,
                "postgresql://a:b@h:9/acme_demo?sslmode=require"
            );

            let shared_file = maps(&[(
                SHARED_URL_VARIABLE,
                &format!("{scheme}://a:b@h:9/file-shared"),
            )]);
            assert_eq!(
                resolve_database_settings_from(&BTreeMap::new(), &shared_file)
                    .unwrap()
                    .database_name,
                ACME_DATABASE_NAME
            );
        }

        let default = resolve_database_settings_from(&BTreeMap::new(), &BTreeMap::new()).unwrap();
        assert_eq!(default.url, DEFAULT_DATABASE_URL);
        assert_eq!(default.psycopg_dsn, DEFAULT_DATABASE_URL);
    }

    #[test]
    fn every_external_resolution_rule_rejects_an_invalid_scheme() {
        let invalid = "mysql://a:b@h:9/acme_demo";
        let direct_env = maps(&[(ACME_URL_VARIABLE, invalid)]);
        let direct_file = maps(&[(ACME_URL_VARIABLE, invalid)]);
        let shared_env = maps(&[(SHARED_URL_VARIABLE, invalid)]);
        let shared_file = maps(&[(SHARED_URL_VARIABLE, invalid)]);

        assert!(resolve_database_settings_from(&direct_env, &BTreeMap::new()).is_err());
        assert!(resolve_database_settings_from(&BTreeMap::new(), &direct_file).is_err());
        assert!(resolve_database_settings_from(&shared_env, &BTreeMap::new()).is_err());
        assert!(resolve_database_settings_from(&BTreeMap::new(), &shared_file).is_err());
        assert!(validate_url(invalid, "built-in default").is_err());
    }

    #[test]
    fn shared_rewrite_preserves_authority_query_and_only_changes_name() {
        let source = maps(&[(
            SHARED_URL_VARIABLE,
            "postgresql+psycopg://user:p%40ss@example:5432/horsies?sslmode=require",
        )]);
        let settings = resolve_database_settings_from(&source, &BTreeMap::new()).unwrap();
        assert_eq!(
            settings.url,
            "postgresql+psycopg://user:p%40ss@example:5432/acme_demo?sslmode=require"
        );
        assert_eq!(
            settings.maintenance_dsn,
            "postgresql://user:p%40ss@example:5432/postgres?sslmode=require"
        );
    }

    #[test]
    fn invalid_scheme_and_missing_name_fail_closed() {
        let bad = maps(&[(ACME_URL_VARIABLE, "mysql://localhost/acme_demo")]);
        assert!(resolve_database_settings_from(&bad, &BTreeMap::new()).is_err());
        let missing = maps(&[(ACME_URL_VARIABLE, "postgresql://localhost")]);
        assert!(resolve_database_settings_from(&missing, &BTreeMap::new()).is_err());
    }
}

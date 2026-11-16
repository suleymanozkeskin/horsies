//! The typed manifest that a client passes to the core once per handle.
//!
//! The client validates its own user config (Pydantic in Python, the config
//! types of the Rust client) and resolves it into a manifest. The manifest
//! holds resolved values only: no field has a default, and an unknown field
//! is refused. A manifest that this parser refuses is a defect of the client,
//! not a user error, so the refusal names the field path and the client
//! crashes loud with it.
//!
//! A refusal never holds an input value: a database URL can carry a password,
//! so the cause names the error class and the position in the text only.

use serde::{Deserialize, Serialize};

use crate::core::{mask_database_url, PostgresConfig};
use crate::manifest_schema::{IntBound, ManifestValue, PythonValue};

/// The one manifest version this core reads.
pub const PRODUCER_MANIFEST_VERSION: u32 = 1;

/// Largest connection count of one pool (`pool_size` and `max_overflow`).
pub const MAX_POOL_CONNECTIONS: u32 = 10_000;

/// Largest idempotency key reservation window: 30 days, in seconds
/// (Python `core/history/identity/keys.py`, "greater than 0, at most 30 days").
pub const MAX_RESERVATION_WINDOW_SECONDS: u32 = 30 * 24 * 60 * 60;

/// The manifest of a process that sends tasks and reads their state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct ProducerManifest {
    /// Always `PRODUCER_MANIFEST_VERSION`, the one version this core reads.
    pub manifest_version: ManifestVersion,
    pub database: DatabaseManifest,
    pub schema: SchemaMode,
    pub send: SendManifest,
}

/// The manifest version. Only the parser constructs it, and it accepts only
/// [`PRODUCER_MANIFEST_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct ManifestVersion(u32);

impl ManifestVersion {
    /// The version this core reads.
    pub const CURRENT: Self = Self(PRODUCER_MANIFEST_VERSION);

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for ManifestVersion {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            PRODUCER_MANIFEST_VERSION => Ok(Self(value)),
            other => Err(format!(
                "manifest version {other} is not supported; this core reads version \
                 {PRODUCER_MANIFEST_VERSION}"
            )),
        }
    }
}

impl From<ManifestVersion> for u32 {
    fn from(version: ManifestVersion) -> Self {
        version.0
    }
}

impl ManifestValue for ManifestVersion {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "MANIFEST_VERSION",
        min: PRODUCER_MANIFEST_VERSION as u64,
        max: PRODUCER_MANIFEST_VERSION as u64,
    });
}

/// Connection settings of the main pool and the session pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct DatabaseManifest {
    pub database_url: DatabaseUrl,
    pub session_database: SessionDatabase,
    /// The main URL points at a PgBouncer transaction-pool endpoint.
    pub pgbouncer_transaction_mode: bool,
    /// Ping a pooled connection before use (Python's rule: only after it
    /// waited in the pool for more than 1 s).
    pub pool_pre_ping: bool,
    /// Connections the pool keeps. Always a number: the core bounds its
    /// connections, so SQLAlchemy's `pool_size=0` (no limit) has no form here.
    pub pool_size: PoolSize,
    /// Connections past `pool_size`. Always a number: SQLAlchemy's
    /// `max_overflow=-1` (no limit) has no form here.
    pub max_overflow: PoolConnections,
    /// The longest a checkout waits for a free connection. 0 means a checkout
    /// does not wait: it fails at once when no connection is free, as
    /// SQLAlchemy's `pool_timeout=0`.
    pub pool_timeout: WholeSeconds,
    pub pool_recycle: PoolRecycle,
    /// Log every statement through the log bridge.
    pub echo: bool,
}

/// Where the session pool (schema work, LISTEN/NOTIFY) connects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum SessionDatabase {
    /// The session pool uses `database_url`.
    SameAsDatabaseUrl,
    /// The session pool uses its own URL.
    Separate { url: DatabaseUrl },
}

/// A PostgreSQL connection URL. It can hold a password, so `Debug` masks it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DatabaseUrl(String);

impl DatabaseUrl {
    /// The URL text, for the connection layer only. Never log it.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl ManifestValue for DatabaseUrl {
    const PYTHON: PythonValue = PythonValue::Str;
}

impl std::fmt::Debug for DatabaseUrl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("DatabaseUrl")
            .field(&mask_database_url(&self.0))
            .finish()
    }
}

/// The size of a pool: 1 to [`MAX_POOL_CONNECTIONS`] connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct PoolSize(u32);

impl PoolSize {
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for PoolSize {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1..=MAX_POOL_CONNECTIONS => Ok(Self(value)),
            _ => Err(format!(
                "a pool size must be from 1 to {MAX_POOL_CONNECTIONS}; the core always bounds its \
                 connections"
            )),
        }
    }
}

impl From<PoolSize> for u32 {
    fn from(size: PoolSize) -> Self {
        size.0
    }
}

impl ManifestValue for PoolSize {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "POOL_SIZE",
        min: 1,
        max: MAX_POOL_CONNECTIONS as u64,
    });
}

/// A connection count from 0 to [`MAX_POOL_CONNECTIONS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct PoolConnections(u32);

impl PoolConnections {
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for PoolConnections {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0..=MAX_POOL_CONNECTIONS => Ok(Self(value)),
            _ => Err(format!(
                "a connection count must be from 0 to {MAX_POOL_CONNECTIONS}"
            )),
        }
    }
}

impl From<PoolConnections> for u32 {
    fn from(count: PoolConnections) -> Self {
        count.0
    }
}

impl ManifestValue for PoolConnections {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "POOL_CONNECTIONS",
        min: 0,
        max: MAX_POOL_CONNECTIONS as u64,
    });
}

/// A duration in whole seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WholeSeconds(u32);

impl WholeSeconds {
    /// Every `u32` is a duration in whole seconds.
    pub const fn from_secs(seconds: u32) -> Self {
        Self(seconds)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

/// When the pool replaces a connection by its age (SQLAlchemy's
/// `pool_recycle`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum PoolRecycle {
    /// A connection is never replaced by age (SQLAlchemy's `pool_recycle=-1`).
    Never,
    /// A connection older than `seconds` is replaced at its next checkout; 0
    /// replaces it at every checkout.
    AfterSeconds { seconds: WholeSeconds },
}

impl PoolRecycle {
    pub const fn after_seconds(seconds: u32) -> Self {
        Self::AfterSeconds {
            seconds: WholeSeconds::from_secs(seconds),
        }
    }
}

impl ManifestValue for WholeSeconds {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "WHOLE_SECONDS",
        min: 0,
        max: u32::MAX as u64,
    });
}

/// Schema behaviour of a handle.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaMode {
    /// The first operation applies the migrations and checks the schema
    /// version.
    MigrateAndValidate,
    /// The handle never runs schema DDL (monitoring reads).
    ObserveOnly,
}

/// Send policy of a handle.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, horsies_macros::ManifestSchema,
)]
#[serde(deny_unknown_fields)]
pub struct SendManifest {
    /// Keep the rerun input at enqueue when the task does not choose.
    pub retain_rerun_input_default: bool,
    pub idempotency_reservation_window: ReservationWindowSeconds,
}

/// The idempotency key reservation window, 1 s to 30 days.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct ReservationWindowSeconds(u32);

impl ReservationWindowSeconds {
    pub const fn get(self) -> u32 {
        self.0
    }

    /// The window as the duration type of the broker.
    pub fn as_duration(self) -> chrono::Duration {
        chrono::Duration::seconds(i64::from(self.0))
    }
}

impl TryFrom<u32> for ReservationWindowSeconds {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1..=MAX_RESERVATION_WINDOW_SECONDS => Ok(Self(value)),
            _ => Err(format!(
                "the reservation window must be from 1 to {MAX_RESERVATION_WINDOW_SECONDS} seconds"
            )),
        }
    }
}

impl From<ReservationWindowSeconds> for u32 {
    fn from(window: ReservationWindowSeconds) -> Self {
        window.0
    }
}

impl ManifestValue for ReservationWindowSeconds {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "RESERVATION_WINDOW_SECONDS",
        min: 1,
        max: MAX_RESERVATION_WINDOW_SECONDS as u64,
    });
}

impl DatabaseManifest {
    /// The broker config of these settings. `retain_rerun_input_default`
    /// comes from the send section of the same manifest.
    pub fn to_postgres_config(&self, retain_rerun_input_default: bool) -> PostgresConfig {
        let session_database_url = match &self.session_database {
            SessionDatabase::SameAsDatabaseUrl => None,
            SessionDatabase::Separate { url } => Some(url.expose().to_owned()),
        };
        PostgresConfig {
            database_url: self.database_url.expose().to_owned(),
            session_database_url,
            pgbouncer_transaction_mode: self.pgbouncer_transaction_mode,
            pool_pre_ping: self.pool_pre_ping,
            pool_size: self.pool_size.get(),
            max_overflow: self.max_overflow.get(),
            retain_rerun_input_default,
            pool_timeout: self.pool_timeout.get(),
            pool_recycle: self.pool_recycle,
            echo: self.echo,
        }
    }
}

/// Why the parser refused a manifest: the field path and the error class.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("manifest refused at {field_path}: {cause}")]
pub struct ManifestRefused {
    /// The path of the field, for example `database.pool_size`, or `.` for
    /// the document itself.
    pub field_path: String,
    /// The error class and the position in the text; never the value.
    pub cause: String,
}

/// Parse a producer manifest from its JSON text.
///
/// Pure: no I/O. Refuses unknown fields, missing fields, values outside
/// their bounds and every manifest version but [`PRODUCER_MANIFEST_VERSION`].
pub fn parse_producer_manifest(text: &str) -> Result<ProducerManifest, ManifestRefused> {
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let parsed: Result<ProducerManifest, _> = serde_path_to_error::deserialize(&mut deserializer);
    let manifest = parsed.map_err(|error| {
        let field_path = error.path().to_string();
        let inner = error.into_inner();
        ManifestRefused {
            field_path,
            cause: refusal_cause(&inner),
        }
    })?;
    deserializer.end().map_err(|error| ManifestRefused {
        field_path: ".".to_owned(),
        cause: refusal_cause(&error),
    })?;
    Ok(manifest)
}

/// The class and position of a JSON error, without the text of the value.
fn refusal_cause(error: &serde_json::Error) -> String {
    let class = match error.classify() {
        serde_json::error::Category::Io => "read error",
        serde_json::error::Category::Syntax => "JSON syntax error",
        serde_json::error::Category::Data => "value does not fit the field",
        serde_json::error::Category::Eof => "text ends early",
    };
    format!("{class} at line {} column {}", error.line(), error.column())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_text(session: &str, window: u32) -> String {
        format!(
            r#"{{
                "manifest_version": 1,
                "database": {{
                    "database_url": "postgresql://user:secret@localhost:5432/db",
                    "session_database": {session},
                    "pgbouncer_transaction_mode": false,
                    "pool_pre_ping": true,
                    "pool_size": 30,
                    "max_overflow": 30,
                    "pool_timeout": 30,
                    "pool_recycle": {{"kind": "AFTER_SECONDS", "seconds": 1800}},
                    "echo": false
                }},
                "schema": "MIGRATE_AND_VALIDATE",
                "send": {{
                    "retain_rerun_input_default": false,
                    "idempotency_reservation_window": {window}
                }}
            }}"#
        )
    }

    #[test]
    fn a_complete_manifest_parses() {
        let text = manifest_text(r#"{"kind": "SAME_AS_DATABASE_URL"}"#, 86_400);
        let manifest = parse_producer_manifest(&text).expect("the manifest is complete");
        assert_eq!(manifest.manifest_version, ManifestVersion::CURRENT);
        assert_eq!(manifest.schema, SchemaMode::MigrateAndValidate);
        assert_eq!(manifest.send.idempotency_reservation_window.get(), 86_400);
        let config = manifest.database.to_postgres_config(false);
        assert_eq!(config.session_database_url, None);
        assert_eq!(config.pool_size, 30);
    }

    #[test]
    fn a_separate_session_url_reaches_the_broker_config() {
        let text = manifest_text(
            r#"{"kind": "SEPARATE", "url": "postgresql://user:secret@direct:5432/db"}"#,
            60,
        );
        let manifest = parse_producer_manifest(&text).expect("the manifest is complete");
        let config = manifest.database.to_postgres_config(true);
        assert_eq!(
            config.session_database_url.as_deref(),
            Some("postgresql://user:secret@direct:5432/db")
        );
        assert!(config.retain_rerun_input_default);
    }

    #[test]
    fn an_unknown_field_is_refused_with_its_path() {
        let text = manifest_text(r#"{"kind": "SAME_AS_DATABASE_URL"}"#, 60).replacen(
            "\"echo\": false",
            "\"echo\": false, \"extra\": 1",
            1,
        );
        let refused = parse_producer_manifest(&text).expect_err("an unknown field is refused");
        assert_eq!(refused.field_path, "database.extra");
    }

    #[test]
    fn a_missing_field_is_refused() {
        let text = manifest_text(r#"{"kind": "SAME_AS_DATABASE_URL"}"#, 60).replacen(
            "\"pool_recycle\": {\"kind\": \"AFTER_SECONDS\", \"seconds\": 1800},",
            "",
            1,
        );
        let refused = parse_producer_manifest(&text).expect_err("a missing field is refused");
        assert!(refused.field_path.starts_with("database"));
    }

    /// The manifest text with one database field set to `value`.
    fn with_database_field(field: &str, value: &str) -> String {
        let text = manifest_text(r#"{"kind": "SAME_AS_DATABASE_URL"}"#, 60);
        let start = text
            .find(&format!("\"{field}\": "))
            .unwrap_or_else(|| panic!("{field} is in the manifest text"));
        let value_start = start + field.len() + 4;
        let line_end = value_start
            + text[value_start..]
                .find('\n')
                .expect("each field ends its line");
        let comma = match text[value_start..line_end].trim_end().ends_with(',') {
            true => ",",
            false => "",
        };
        format!(
            "{}{value}{comma}{}",
            &text[..value_start],
            &text[line_end..]
        )
    }

    fn database_config(field: &str, value: &str) -> PostgresConfig {
        let text = with_database_field(field, value);
        parse_producer_manifest(&text)
            .unwrap_or_else(|refused| panic!("{field} = {value} is accepted: {refused}"))
            .database
            .to_postgres_config(false)
    }

    fn refused_path(field: &str, value: &str) -> String {
        parse_producer_manifest(&with_database_field(field, value))
            .expect_err("the value is refused")
            .field_path
    }

    #[test]
    fn a_pool_size_is_a_number_of_connections() {
        assert_eq!(database_config("pool_size", "1").pool_size, 1);
        assert_eq!(
            database_config("pool_size", &MAX_POOL_CONNECTIONS.to_string()).pool_size,
            MAX_POOL_CONNECTIONS
        );
    }

    #[test]
    fn a_pool_size_of_zero_is_refused_because_the_core_bounds_its_connections() {
        assert_eq!(refused_path("pool_size", "0"), "database.pool_size");
    }

    #[test]
    fn a_pool_size_past_its_bound_or_below_zero_is_refused() {
        let past = (MAX_POOL_CONNECTIONS + 1).to_string();
        assert_eq!(refused_path("pool_size", &past), "database.pool_size");
        assert_eq!(refused_path("pool_size", "-1"), "database.pool_size");
    }

    #[test]
    fn an_overflow_of_zero_means_no_overflow() {
        assert_eq!(database_config("max_overflow", "0").max_overflow, 0);
    }

    #[test]
    fn an_unlimited_overflow_is_refused() {
        assert_eq!(refused_path("max_overflow", "-1"), "database.max_overflow");
    }

    #[test]
    fn a_pool_timeout_of_zero_means_a_checkout_does_not_wait() {
        assert_eq!(database_config("pool_timeout", "0").pool_timeout, 0);
    }

    #[test]
    fn a_negative_pool_timeout_is_refused() {
        assert_eq!(refused_path("pool_timeout", "-1"), "database.pool_timeout");
    }

    #[test]
    fn a_pool_that_never_recycles_reaches_the_broker_config() {
        assert_eq!(
            database_config("pool_recycle", r#"{"kind": "NEVER"}"#).pool_recycle,
            PoolRecycle::Never
        );
    }

    #[test]
    fn a_recycle_age_of_zero_replaces_a_connection_at_every_checkout() {
        assert_eq!(
            database_config("pool_recycle", r#"{"kind": "AFTER_SECONDS", "seconds": 0}"#)
                .pool_recycle,
            PoolRecycle::after_seconds(0)
        );
    }

    #[test]
    fn a_recycle_age_without_meaning_is_refused() {
        assert!(refused_path(
            "pool_recycle",
            r#"{"kind": "AFTER_SECONDS", "seconds": -5}"#
        )
        .starts_with("database.pool_recycle"));
        // The bare number of SQLAlchemy is not a manifest form.
        assert!(refused_path("pool_recycle", "-1").starts_with("database.pool_recycle"));
    }

    #[test]
    fn another_manifest_version_is_refused() {
        let text = manifest_text(r#"{"kind": "SAME_AS_DATABASE_URL"}"#, 60).replacen(
            "\"manifest_version\": 1",
            "\"manifest_version\": 2",
            1,
        );
        let refused = parse_producer_manifest(&text).expect_err("version 2 is refused");
        assert_eq!(refused.field_path, "manifest_version");
    }

    #[test]
    fn a_window_outside_its_bound_is_refused() {
        for window in [0, MAX_RESERVATION_WINDOW_SECONDS + 1] {
            let text = manifest_text(r#"{"kind": "SAME_AS_DATABASE_URL"}"#, window);
            let refused = parse_producer_manifest(&text).expect_err("a window outside 1..=30 days");
            assert_eq!(refused.field_path, "send.idempotency_reservation_window");
        }
    }

    #[test]
    fn a_refusal_never_holds_the_url_password() {
        let text = manifest_text(r#"{"kind": "SAME_AS_DATABASE_URL"}"#, 60).replacen(
            "\"pool_size\": 30",
            "\"pool_size\": \"postgresql://u:secret@h/d\"",
            1,
        );
        let refused = parse_producer_manifest(&text).expect_err("a string is not a count");
        assert!(!refused.to_string().contains("secret"));
        assert_eq!(refused.field_path, "database.pool_size");
    }

    #[test]
    fn trailing_text_after_the_manifest_is_refused() {
        let text = format!(
            "{} []",
            manifest_text(r#"{"kind": "SAME_AS_DATABASE_URL"}"#, 60)
        );
        let refused = parse_producer_manifest(&text).expect_err("trailing text is refused");
        assert_eq!(refused.field_path, ".");
    }

    #[test]
    fn the_debug_form_masks_the_password() {
        let text = manifest_text(r#"{"kind": "SAME_AS_DATABASE_URL"}"#, 60);
        let manifest = parse_producer_manifest(&text).expect("the manifest is complete");
        assert!(!format!("{manifest:?}").contains("secret"));
    }
}

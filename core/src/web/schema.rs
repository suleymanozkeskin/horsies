//! Read-only schema compatibility probing for monitoring actions.

use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::broker::core_rows::{column, one_row};
use crate::broker::{expected_schema_version, BrokerError, PostgresBroker, MIGRATIONS_TABLE};
use crate::db::{ParameterType, TypedStatement};

/// Whether the migration ledger (`$1`) and the 0.x Python bookkeeping table
/// exist.
static SCHEMA_TABLES: TypedStatement = TypedStatement::new(
    "web_schema_tables",
    "SELECT to_regclass($1) IS NOT NULL AS migrations_table,
            to_regclass('horsies_schema_version') IS NOT NULL AS schema_version_table",
    &[ParameterType::Text],
);

static STORED_VERSION_SQL: LazyLock<String> = LazyLock::new(|| {
    format!("SELECT COALESCE(max(version), 0) AS version FROM {MIGRATIONS_TABLE} WHERE success")
});

/// The highest successful version of the migration ledger.
static STORED_VERSION: LazyLock<TypedStatement> = LazyLock::new(|| {
    TypedStatement::new(
        "web_schema_stored_version",
        STORED_VERSION_SQL.as_str(),
        &[],
    )
});

pub const SCHEMA_TTL: Duration = Duration::from_secs(60);
pub const SCHEMA_INCOMPATIBLE: &str = "SCHEMA_INCOMPATIBLE";
pub const SCHEMA_UNKNOWN: &str = "SCHEMA_UNKNOWN";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaState {
    Match,
    Mismatch,
    Absent,
    /// A horsies 0.x schema: `horsies_schema_version` without
    /// `horsies_migrations` (Python 0.x), or a highest successful version
    /// below the baseline's (horsies-rust 0.x, whose versions end at 65).
    /// This build neither reads nor upgrades it.
    ZeroX,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaStatus {
    pub state: SchemaState,
    pub version: Option<i64>,
    pub expected_version: i64,
}

impl SchemaStatus {
    pub const fn compatible(self) -> bool {
        matches!(self.state, SchemaState::Match)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaIncompatible {
    pub status: SchemaStatus,
    pub code: &'static str,
    pub detail: String,
}

impl SchemaIncompatible {
    pub fn from_status(status: SchemaStatus) -> Self {
        let (code, detail) = match status.state {
            SchemaState::Unknown => (
                SCHEMA_UNKNOWN,
                "Cannot reach the database to determine its schema state, so actions are unavailable."
                    .to_owned(),
            ),
            SchemaState::Absent => (
                SCHEMA_INCOMPATIBLE,
                "This database has no horsies schema, so actions are unavailable. Start a horsies app or worker to initialize it; the monitoring tool never modifies the database schema."
                    .to_owned(),
            ),
            SchemaState::ZeroX => (
                SCHEMA_INCOMPATIBLE,
                "This database holds a horsies 0.x schema, so actions are unavailable. This build does not read or upgrade a 0.x database: drain it and point the fleet at a new database."
                    .to_owned(),
            ),
            SchemaState::Mismatch => (
                SCHEMA_INCOMPATIBLE,
                format!(
                    "Database schema is v{}; this build expects v{}. Actions are disabled until the versions match.",
                    status.version.unwrap_or_default(),
                    status.expected_version,
                ),
            ),
            SchemaState::Match => (SCHEMA_INCOMPATIBLE, String::new()),
        };
        Self {
            status,
            code,
            detail,
        }
    }
}

#[async_trait]
pub(crate) trait SchemaReader: Send + Sync {
    async fn read(&self, expected_version: i64) -> Result<SchemaStatus, BrokerError>;
}

struct BrokerSchemaReader {
    broker: Arc<PostgresBroker>,
}

#[async_trait]
impl SchemaReader for BrokerSchemaReader {
    async fn read(&self, expected_version: i64) -> Result<SchemaStatus, BrokerError> {
        let rows = self
            .broker
            .query_main(&SCHEMA_TABLES.bind().value(&MIGRATIONS_TABLE).finish())
            .await?;
        let row = one_row(rows, &SCHEMA_TABLES)?;
        let migrations_table: bool = column(&row, &SCHEMA_TABLES, "migrations_table")?;
        let schema_version_table: bool = column(&row, &SCHEMA_TABLES, "schema_version_table")?;
        if !migrations_table {
            return Ok(classify_without_migrations(
                schema_version_table,
                expected_version,
            ));
        }

        let statement: &TypedStatement = &STORED_VERSION;
        let rows = self.broker.query_main(&statement.bind().finish()).await?;
        let row = one_row(rows, statement)?;
        let stored: i64 = column(&row, statement, "version")?;
        Ok(classify_stored_version(stored, expected_version))
    }
}

/// The verdict for a database without `horsies_migrations` (Python
/// `classify_without_migrations`, `python/horsies/web/schema.py`):
/// `horsies_schema_version` is the bookkeeping of horsies 0.x Python, so with
/// it the database holds a 0.x schema; without it the database has no
/// horsies schema. The 0.x version is not read.
fn classify_without_migrations(schema_version_table: bool, expected_version: i64) -> SchemaStatus {
    SchemaStatus {
        state: match schema_version_table {
            true => SchemaState::ZeroX,
            false => SchemaState::Absent,
        },
        version: None,
        expected_version,
    }
}

/// The verdict for the highest successful version in `horsies_migrations`
/// (Python `classify_stored_version`): 0 is no schema; a version below the
/// baseline's is horsies-rust 0.x bookkeeping, because the first version of
/// this schema line is the baseline; otherwise the version matches or
/// differs from the one this build expects.
fn classify_stored_version(stored: i64, expected_version: i64) -> SchemaStatus {
    let state = match stored {
        0 => {
            return SchemaStatus {
                state: SchemaState::Absent,
                version: None,
                expected_version,
            }
        }
        _ if stored < crate::broker::migrations::BASELINE_MIGRATION_VERSION => SchemaState::ZeroX,
        _ if stored == expected_version => SchemaState::Match,
        _ => SchemaState::Mismatch,
    };
    SchemaStatus {
        state,
        version: Some(stored),
        expected_version,
    }
}

#[derive(Debug, Clone, Copy)]
struct CachedStatus {
    status: SchemaStatus,
    expires_at: Instant,
}

pub struct SchemaProbe {
    reader: Arc<dyn SchemaReader>,
    expected_version: i64,
    ttl: Duration,
    cached: RwLock<Option<CachedStatus>>,
    refresh: Mutex<()>,
}

impl SchemaProbe {
    pub fn new(broker: Arc<PostgresBroker>) -> Self {
        Self::with_reader(
            Arc::new(BrokerSchemaReader { broker }),
            expected_schema_version(),
            SCHEMA_TTL,
        )
    }

    pub(crate) fn with_reader(
        reader: Arc<dyn SchemaReader>,
        expected_version: i64,
        ttl: Duration,
    ) -> Self {
        Self {
            reader,
            expected_version,
            ttl,
            cached: RwLock::new(None),
            refresh: Mutex::new(()),
        }
    }

    pub async fn status(&self) -> SchemaStatus {
        let now = Instant::now();
        if let Some(cached) = *self.cached.read().await {
            if now < cached.expires_at {
                return cached.status;
            }
        }

        let _refresh = self.refresh.lock().await;
        let now = Instant::now();
        if let Some(cached) = *self.cached.read().await {
            if now < cached.expires_at {
                return cached.status;
            }
        }

        match self.reader.read(self.expected_version).await {
            Ok(status) => {
                *self.cached.write().await = Some(CachedStatus {
                    status,
                    expires_at: now + self.ttl,
                });
                status
            }
            Err(error) => {
                tracing::warn!(error = %error, "Schema version probe failed: {error}");
                self.cached
                    .read()
                    .await
                    .map(|cached| cached.status)
                    .unwrap_or(SchemaStatus {
                        state: SchemaState::Unknown,
                        version: None,
                        expected_version: self.expected_version,
                    })
            }
        }
    }
}

#[cfg(test)]
mod classify_tests {
    use super::*;

    const EXPECTED: i64 = crate::broker::migrations::BASELINE_MIGRATION_VERSION;

    /// Python `test_web_schema_probe.py`: `horsies_schema_version` without
    /// `horsies_migrations` is a 0.x Python schema; neither table is no schema.
    #[test]
    fn a_database_without_the_ledger_is_zero_x_or_absent() {
        assert_eq!(
            classify_without_migrations(true, EXPECTED).state,
            SchemaState::ZeroX
        );
        assert_eq!(
            classify_without_migrations(false, EXPECTED).state,
            SchemaState::Absent
        );
        assert_eq!(classify_without_migrations(true, EXPECTED).version, None);
    }

    /// A version below the baseline is horsies-rust 0.x bookkeeping.
    #[test]
    fn the_stored_version_decides_the_state() {
        assert_eq!(
            classify_stored_version(0, EXPECTED).state,
            SchemaState::Absent
        );
        assert_eq!(classify_stored_version(0, EXPECTED).version, None);
        for rust_zero_x in [1, 65, EXPECTED - 1] {
            let status = classify_stored_version(rust_zero_x, EXPECTED);
            assert_eq!(status.state, SchemaState::ZeroX, "{rust_zero_x}");
            assert_eq!(status.version, Some(rust_zero_x));
        }
        assert_eq!(
            classify_stored_version(EXPECTED, EXPECTED).state,
            SchemaState::Match
        );
        assert_eq!(
            classify_stored_version(EXPECTED + 1, EXPECTED).state,
            SchemaState::Mismatch
        );
    }

    #[test]
    fn a_zero_x_database_disables_actions_with_python_s_detail() {
        let refused = SchemaIncompatible::from_status(classify_without_migrations(true, EXPECTED));

        assert_eq!(refused.code, SCHEMA_INCOMPATIBLE);
        assert!(
            refused.detail.contains("horsies 0.x schema"),
            "{}",
            refused.detail
        );
        assert_eq!(
            serde_json::to_string(&SchemaState::ZeroX).unwrap(),
            "\"ZERO_X\""
        );
    }
}

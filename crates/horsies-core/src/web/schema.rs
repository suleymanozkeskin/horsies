//! Read-only schema compatibility probing for monitoring actions.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::broker::{expected_schema_version, PostgresBroker, MIGRATIONS_TABLE};
use crate::core::history::cutover::state::{CUTOVER_NAME, CUTOVER_STATE_TABLE};

pub const SCHEMA_TTL: Duration = Duration::from_secs(60);
pub const SCHEMA_INCOMPATIBLE: &str = "SCHEMA_INCOMPATIBLE";
pub const SCHEMA_UNKNOWN: &str = "SCHEMA_UNKNOWN";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaState {
    Match,
    Mismatch,
    CutoverRequired,
    Absent,
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
            SchemaState::CutoverRequired => (
                SCHEMA_INCOMPATIBLE,
                format!(
                    "Database schema is v{}, but the offline task-history cutover is incomplete. Run the documented cutover stages through tighten and validation before enabling actions.",
                    status.version.unwrap_or_default(),
                ),
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
    async fn read(&self, expected_version: i64) -> Result<SchemaStatus, sqlx::Error>;
}

struct BrokerSchemaReader {
    broker: Arc<PostgresBroker>,
}

#[async_trait]
impl SchemaReader for BrokerSchemaReader {
    async fn read(&self, expected_version: i64) -> Result<SchemaStatus, sqlx::Error> {
        let migrations_present: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(MIGRATIONS_TABLE)
            .fetch_one(self.broker.pool())
            .await?;
        if !migrations_present {
            return Ok(SchemaStatus {
                state: SchemaState::Absent,
                version: None,
                expected_version,
            });
        }

        let stored: i64 = sqlx::query_scalar(&format!(
            "SELECT COALESCE(max(version), 0) FROM {MIGRATIONS_TABLE} WHERE success"
        ))
        .fetch_one(self.broker.pool())
        .await?;
        if stored == 0 {
            return Ok(SchemaStatus {
                state: SchemaState::Absent,
                version: None,
                expected_version,
            });
        }
        if stored != expected_version {
            return Ok(SchemaStatus {
                state: SchemaState::Mismatch,
                version: Some(stored),
                expected_version,
            });
        }

        let cutover_table_present: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(CUTOVER_STATE_TABLE)
            .fetch_one(self.broker.pool())
            .await?;
        let cutover_complete = if cutover_table_present {
            sqlx::query_scalar::<_, bool>(&format!(
                "SELECT EXISTS (SELECT 1 FROM {CUTOVER_STATE_TABLE} WHERE cutover_name = $1)"
            ))
            .bind(CUTOVER_NAME)
            .fetch_one(self.broker.pool())
            .await?
        } else {
            false
        };
        Ok(SchemaStatus {
            state: if cutover_complete {
                SchemaState::Match
            } else {
                SchemaState::CutoverRequired
            },
            version: Some(stored),
            expected_version,
        })
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
                tracing::warn!(error = %error, "monitoring schema probe failed");
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

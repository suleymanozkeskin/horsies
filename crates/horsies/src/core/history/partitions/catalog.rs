//! Fail-closed reads of the durable leaf catalog and PostgreSQL catalogs.

use chrono::{DateTime, Duration, Utc};
use sqlx::{FromRow, PgConnection};

use crate::core::history::commands::is_safe_identifier;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{HEARTBEAT_CLASS_KEY, LEAF_CATALOG, RETENTION_CLASSES};

pub const INDEX_SCHEMA_VERSION: i16 = 1;
pub const ORDERING_INDEX_COLUMN: &str = "enqueued_at";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionClassRow {
    pub class_key: String,
    pub duration: Option<Duration>,
    pub partition_interval: Option<Duration>,
    pub finite_parent_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct LeafCatalogRow {
    pub leaf_name: String,
    pub parent_name: String,
    pub class_key: String,
    pub lower_anchor: DateTime<Utc>,
    pub upper_anchor: DateTime<Utc>,
    pub index_schema_version: i16,
    pub id_index_name: String,
    pub partition_bound: String,
    pub min_birth_at: Option<DateTime<Utc>>,
    pub min_birth_verified: bool,
    pub created_at: DateTime<Utc>,
    pub detached_at: Option<DateTime<Utc>>,
    pub dropped_at: Option<DateTime<Utc>>,
}

impl LeafCatalogRow {
    fn validate(self) -> Result<Self, HistoryError> {
        for (label, value) in [
            ("leaf", self.leaf_name.as_str()),
            ("parent", self.parent_name.as_str()),
            ("id index", self.id_index_name.as_str()),
        ] {
            if !is_safe_identifier(value) {
                return Err(HistoryError::contract(format!(
                    "cataloged {label} name is not a safe identifier: {value:?}"
                )));
            }
        }
        if self.class_key.is_empty() || self.lower_anchor >= self.upper_anchor {
            return Err(HistoryError::contract(
                "leaf catalog row carries an invalid class or bounds",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafPhysicalState {
    pub relation_exists: bool,
    pub parent_exists: bool,
    pub partition_bound: Option<String>,
    pub id_index_exists: bool,
    pub detach_pending: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestLeafSelection {
    pub attached: Vec<LeafCatalogRow>,
    pub absent_relations: Vec<String>,
}

#[derive(FromRow)]
struct RetentionClassRaw {
    class_key: String,
    duration_us: Option<i64>,
    partition_interval_us: Option<i64>,
    finite_parent_name: Option<String>,
}

pub async fn read_retention_class(
    connection: &mut PgConnection,
    class_key: &str,
) -> Result<Option<RetentionClassRow>, HistoryError> {
    let sql = format!(
        "SELECT class_key,
                (EXTRACT(epoch FROM duration) * 1000000)::bigint AS duration_us,
                (EXTRACT(epoch FROM partition_interval) * 1000000)::bigint
                    AS partition_interval_us,
                finite_parent_name
         FROM {RETENTION_CLASSES}
         WHERE class_key = $1"
    );
    let row: Option<RetentionClassRaw> = sqlx::query_as(&sql)
        .bind(class_key)
        .fetch_optional(connection)
        .await?;
    row.map(|row| {
        if row.class_key.is_empty() {
            return Err(HistoryError::contract("retention class key decoded empty"));
        }
        if let Some(parent) = row.finite_parent_name.as_deref() {
            if !is_safe_identifier(parent) {
                return Err(HistoryError::contract(
                    "retention class names an unsafe parent relation",
                ));
            }
        }
        if row.duration_us.is_some() && row.finite_parent_name.is_none() {
            return Err(HistoryError::contract(
                "finite retention class has no finite parent relation",
            ));
        }
        Ok(RetentionClassRow {
            class_key: row.class_key,
            duration: row.duration_us.map(Duration::microseconds),
            partition_interval: row.partition_interval_us.map(Duration::microseconds),
            finite_parent_name: row.finite_parent_name,
        })
    })
    .transpose()
}

const LEAF_COLUMNS: &str = "leaf_name, parent_name, class_key,
    lower_anchor, upper_anchor, index_schema_version, id_index_name,
    partition_bound, min_birth_at, min_birth_verified, created_at,
    detached_at, dropped_at";

pub async fn read_leaf_catalog_row(
    connection: &mut PgConnection,
    leaf_name: &str,
) -> Result<Option<LeafCatalogRow>, HistoryError> {
    let sql = format!("SELECT {LEAF_COLUMNS} FROM {LEAF_CATALOG} WHERE leaf_name = $1");
    sqlx::query_as::<_, LeafCatalogRow>(&sql)
        .bind(leaf_name)
        .fetch_optional(connection)
        .await?
        .map(LeafCatalogRow::validate)
        .transpose()
}

pub async fn read_attached_leaf_rows(
    connection: &mut PgConnection,
    class_key: &str,
) -> Result<Vec<LeafCatalogRow>, HistoryError> {
    read_attached(connection, Some(class_key)).await
}

pub async fn read_all_attached_leaf_rows(
    connection: &mut PgConnection,
) -> Result<Vec<LeafCatalogRow>, HistoryError> {
    read_attached(connection, None).await
}

async fn read_attached(
    connection: &mut PgConnection,
    class_key: Option<&str>,
) -> Result<Vec<LeafCatalogRow>, HistoryError> {
    let class_filter = if class_key.is_some() {
        "AND class_key = $1"
    } else {
        ""
    };
    let sql = format!(
        "SELECT {LEAF_COLUMNS} FROM {LEAF_CATALOG}
         WHERE detached_at IS NULL AND dropped_at IS NULL {class_filter}
         ORDER BY lower_anchor, leaf_name"
    );
    let rows = match class_key {
        Some(class_key) => {
            sqlx::query_as::<_, LeafCatalogRow>(&sql)
                .bind(class_key)
                .fetch_all(connection)
                .await?
        }
        None => {
            sqlx::query_as::<_, LeafCatalogRow>(&sql)
                .fetch_all(connection)
                .await?
        }
    };
    rows.into_iter().map(LeafCatalogRow::validate).collect()
}

#[derive(FromRow)]
struct ManifestRaw {
    leaf_name: String,
    parent_name: String,
    class_key: String,
    lower_anchor: DateTime<Utc>,
    upper_anchor: DateTime<Utc>,
    index_schema_version: i16,
    id_index_name: String,
    partition_bound: String,
    min_birth_at: Option<DateTime<Utc>>,
    min_birth_verified: bool,
    created_at: DateTime<Utc>,
    detached_at: Option<DateTime<Utc>>,
    dropped_at: Option<DateTime<Utc>>,
    relation_exists: bool,
    parent_exists: bool,
}

pub async fn read_manifest_leaf_rows(
    connection: &mut PgConnection,
) -> Result<ManifestLeafSelection, HistoryError> {
    let sql = format!(
        "SELECT {LEAF_COLUMNS},
                to_regclass(leaf_name) IS NOT NULL AS relation_exists,
                to_regclass(parent_name) IS NOT NULL AS parent_exists
         FROM {LEAF_CATALOG}
         WHERE detached_at IS NULL AND dropped_at IS NULL
         ORDER BY lower_anchor, leaf_name"
    );
    let rows: Vec<ManifestRaw> = sqlx::query_as(&sql).fetch_all(connection).await?;
    let mut attached = Vec::with_capacity(rows.len());
    let mut absent = Vec::new();
    let mut history_rows = 0_usize;
    let mut any_history_parent = false;
    for row in rows {
        let relation_exists = row.relation_exists;
        let parent_exists = row.parent_exists;
        let catalog = LeafCatalogRow {
            leaf_name: row.leaf_name,
            parent_name: row.parent_name,
            class_key: row.class_key,
            lower_anchor: row.lower_anchor,
            upper_anchor: row.upper_anchor,
            index_schema_version: row.index_schema_version,
            id_index_name: row.id_index_name,
            partition_bound: row.partition_bound,
            min_birth_at: row.min_birth_at,
            min_birth_verified: row.min_birth_verified,
            created_at: row.created_at,
            detached_at: row.detached_at,
            dropped_at: row.dropped_at,
        }
        .validate()?;
        if catalog.class_key != HEARTBEAT_CLASS_KEY {
            history_rows += 1;
            any_history_parent |= parent_exists;
            if !relation_exists {
                absent.push(catalog.leaf_name.clone());
            }
        }
        attached.push(catalog);
    }
    if history_rows > 0 && !any_history_parent {
        return Err(HistoryError::HistoryParentAbsent(
            "no attached history leaf resolves its parent relation".to_owned(),
        ));
    }
    Ok(ManifestLeafSelection {
        attached,
        absent_relations: absent,
    })
}

pub async fn read_attached_birth_floor(
    connection: &mut PgConnection,
) -> Result<Option<DateTime<Utc>>, HistoryError> {
    let sql = format!(
        "SELECT min(min_birth_at) FROM {LEAF_CATALOG}
         WHERE detached_at IS NULL AND dropped_at IS NULL AND class_key <> $1"
    );
    Ok(sqlx::query_scalar(&sql)
        .bind(HEARTBEAT_CLASS_KEY)
        .fetch_one(connection)
        .await?)
}

pub(crate) async fn pin_utc_timezone(
    connection: &mut PgConnection,
) -> Result<String, HistoryError> {
    let prior: String = sqlx::query_scalar("SHOW timezone")
        .fetch_one(&mut *connection)
        .await?;
    sqlx::query("SELECT set_config('timezone', 'UTC', false)")
        .execute(connection)
        .await?;
    Ok(prior)
}

pub(crate) async fn restore_timezone(
    connection: &mut PgConnection,
    prior: &str,
) -> Result<(), HistoryError> {
    sqlx::query("SELECT set_config('timezone', $1, false)")
        .bind(prior)
        .execute(connection)
        .await?;
    Ok(())
}

pub async fn capture_partition_bound_utc(
    connection: &mut PgConnection,
    leaf_name: &str,
) -> Result<Option<String>, HistoryError> {
    if !is_safe_identifier(leaf_name) {
        return Err(HistoryError::contract("unsafe partition relation name"));
    }
    let prior = pin_utc_timezone(connection).await?;
    let result = sqlx::query_scalar(
        "SELECT pg_get_expr(c.relpartbound, c.oid)
         FROM pg_class AS c WHERE c.oid = to_regclass($1)",
    )
    .bind(leaf_name)
    .fetch_optional(&mut *connection)
    .await;
    let restored = restore_timezone(connection, &prior).await;
    let value: Option<Option<String>> = result?;
    restored?;
    Ok(value.flatten())
}

#[derive(FromRow)]
struct PhysicalRaw {
    leaf_exists: bool,
    parent_exists: bool,
    id_index_exists: bool,
    partition_bound: Option<String>,
    detach_pending: Option<bool>,
}

pub async fn read_leaf_physical_state(
    connection: &mut PgConnection,
    leaf_name: &str,
    parent_name: &str,
    id_index_name: &str,
) -> Result<LeafPhysicalState, HistoryError> {
    if [leaf_name, parent_name, id_index_name]
        .iter()
        .any(|value| !is_safe_identifier(value))
    {
        return Err(HistoryError::contract("unsafe physical-state identifier"));
    }
    let prior = pin_utc_timezone(connection).await?;
    let result = sqlx::query_as::<_, PhysicalRaw>(
        "SELECT to_regclass($1) IS NOT NULL AS leaf_exists,
                to_regclass($2) IS NOT NULL AS parent_exists,
                to_regclass($3) IS NOT NULL AS id_index_exists,
                (SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class AS c
                 WHERE c.oid = to_regclass($1)) AS partition_bound,
                (SELECT i.inhdetachpending FROM pg_inherits AS i
                 WHERE i.inhparent = to_regclass($2)
                   AND i.inhrelid = to_regclass($1)) AS detach_pending",
    )
    .bind(leaf_name)
    .bind(parent_name)
    .bind(id_index_name)
    .fetch_one(&mut *connection)
    .await;
    let restored = restore_timezone(connection, &prior).await;
    let row = result?;
    restored?;
    Ok(LeafPhysicalState {
        relation_exists: row.leaf_exists,
        parent_exists: row.parent_exists,
        partition_bound: row.partition_bound,
        id_index_exists: row.id_index_exists,
        detach_pending: row.detach_pending,
    })
}

pub async fn read_leaf_ordering_index_exists(
    connection: &mut PgConnection,
    leaf_name: &str,
) -> Result<bool, HistoryError> {
    if !is_safe_identifier(leaf_name) {
        return Err(HistoryError::contract("unsafe ordering-index leaf name"));
    }
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pg_index AS i
             JOIN pg_class AS ic ON ic.oid = i.indexrelid
             JOIN pg_am AS am ON am.oid = ic.relam
             WHERE i.indrelid = to_regclass($1)
               AND am.amname = 'btree' AND i.indpred IS NULL
               AND i.indnkeyatts = 1
               AND i.indkey[0] = (
                   SELECT a.attnum FROM pg_attribute AS a
                   WHERE a.attrelid = to_regclass($1) AND a.attname = $2
               )
         )",
    )
    .bind(leaf_name)
    .bind(ORDERING_INDEX_COLUMN)
    .fetch_one(connection)
    .await?)
}

pub async fn database_now(connection: &mut PgConnection) -> Result<DateTime<Utc>, HistoryError> {
    Ok(sqlx::query_scalar("SELECT statement_timestamp()")
        .fetch_one(connection)
        .await?)
}

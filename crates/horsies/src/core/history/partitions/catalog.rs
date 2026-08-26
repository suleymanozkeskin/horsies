//! Fail-closed reads of the durable leaf catalog and PostgreSQL catalogs.

use chrono::{DateTime, Duration, Utc};
use sqlx::{FromRow, PgConnection};

use crate::core::history::commands::{is_safe_identifier, LeafBounds};
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
    pub partition_bound_matches_expected: bool,
    pub id_index_conformant: bool,
    pub detach_pending: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafIndexKind {
    History,
    Heartbeat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafPartitionBoundExpectation<'a> {
    Requested(&'a LeafBounds),
    CatalogOnly,
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
    partition_bound: Option<String>,
    partition_bound_matches_expected: bool,
    id_index_conformant: bool,
    detach_pending: Option<bool>,
}

pub async fn read_leaf_physical_state(
    connection: &mut PgConnection,
    leaf_name: &str,
    parent_name: &str,
    id_index_name: &str,
    bound_expectation: LeafPartitionBoundExpectation<'_>,
    index_kind: LeafIndexKind,
) -> Result<LeafPhysicalState, HistoryError> {
    if [leaf_name, parent_name, id_index_name]
        .iter()
        .any(|value| !is_safe_identifier(value))
    {
        return Err(HistoryError::contract("unsafe physical-state identifier"));
    }
    let heartbeat_index = matches!(index_kind, LeafIndexKind::Heartbeat);
    let (expected_lower, expected_upper) = match bound_expectation {
        LeafPartitionBoundExpectation::Requested(bounds) => {
            (Some(bounds.lower()), Some(bounds.upper()))
        }
        LeafPartitionBoundExpectation::CatalogOnly => (None, None),
    };
    let prior = pin_utc_timezone(connection).await?;
    let result = sqlx::query_as::<_, PhysicalRaw>(
        "WITH selected_index AS MATERIALIZED (
             SELECT index_state.*, access_method.amname
             FROM pg_index AS index_state
             JOIN pg_class AS index_relation
               ON index_relation.oid = index_state.indexrelid
             JOIN pg_am AS access_method
               ON access_method.oid = index_relation.relam
             WHERE index_state.indexrelid = to_regclass($3)
         )
         SELECT to_regclass($1) IS NOT NULL AS leaf_exists,
                to_regclass($2) IS NOT NULL AS parent_exists,
                (SELECT pg_get_expr(relation.relpartbound, relation.oid)
                 FROM pg_class AS relation
                 WHERE relation.oid = to_regclass($1)) AS partition_bound,
                COALESCE((
                    SELECT $4::timestamptz IS NULL
                        OR pg_get_expr(relation.relpartbound, relation.oid) =
                            format(
                                'FOR VALUES FROM (%L) TO (%L)',
                                $4::timestamptz,
                                $5::timestamptz
                            )
                    FROM pg_class AS relation
                    WHERE relation.oid = to_regclass($1)
                ), false) AS partition_bound_matches_expected,
                EXISTS (
                    SELECT 1 FROM selected_index
                    WHERE selected_index.indrelid = to_regclass($1)
                      AND selected_index.amname = 'btree'
                      AND selected_index.indisvalid
                      AND selected_index.indisready
                      AND selected_index.indislive
                      AND NOT selected_index.indisunique
                      AND NOT selected_index.indisprimary
                      AND NOT selected_index.indisexclusion
                      AND selected_index.indpred IS NULL
                      AND selected_index.indexprs IS NULL
                      AND (
                          (
                              NOT $6::boolean
                              AND selected_index.indnkeyatts = 1
                              AND selected_index.indnatts = 1
                              AND selected_index.indkey[0] = (
                                  SELECT attribute.attnum
                                  FROM pg_attribute AS attribute
                                  WHERE attribute.attrelid = to_regclass($1)
                                    AND attribute.attname = 'task_id'
                              )
                              AND selected_index.indcollation[0] = (
                                  SELECT attribute.attcollation
                                  FROM pg_attribute AS attribute
                                  WHERE attribute.attrelid = to_regclass($1)
                                    AND attribute.attname = 'task_id'
                              )
                              AND pg_get_indexdef(
                                  selected_index.indexrelid, 1, false
                              ) = 'task_id'
                              AND pg_get_indexdef(selected_index.indexrelid)
                                  LIKE '% USING btree (task_id)'
                              AND selected_index.indoption[0] = 0
                          )
                          OR
                          (
                              $6::boolean
                              AND selected_index.indnkeyatts = 3
                              AND selected_index.indnatts = 3
                              AND selected_index.indkey[0] = (
                                  SELECT attribute.attnum
                                  FROM pg_attribute AS attribute
                                  WHERE attribute.attrelid = to_regclass($1)
                                    AND attribute.attname = 'task_id'
                              )
                              AND selected_index.indkey[1] = (
                                  SELECT attribute.attnum
                                  FROM pg_attribute AS attribute
                                  WHERE attribute.attrelid = to_regclass($1)
                                    AND attribute.attname = 'role'
                              )
                              AND selected_index.indkey[2] = (
                                  SELECT attribute.attnum
                                  FROM pg_attribute AS attribute
                                  WHERE attribute.attrelid = to_regclass($1)
                                    AND attribute.attname = 'sent_at'
                              )
                              AND selected_index.indcollation[0] = (
                                  SELECT attribute.attcollation
                                  FROM pg_attribute AS attribute
                                  WHERE attribute.attrelid = to_regclass($1)
                                    AND attribute.attname = 'task_id'
                              )
                              AND selected_index.indcollation[1] = (
                                  SELECT attribute.attcollation
                                  FROM pg_attribute AS attribute
                                  WHERE attribute.attrelid = to_regclass($1)
                                    AND attribute.attname = 'role'
                              )
                              AND selected_index.indcollation[2] = (
                                  SELECT attribute.attcollation
                                  FROM pg_attribute AS attribute
                                  WHERE attribute.attrelid = to_regclass($1)
                                    AND attribute.attname = 'sent_at'
                              )
                              AND pg_get_indexdef(
                                  selected_index.indexrelid, 1, false
                              ) = 'task_id'
                              AND pg_get_indexdef(
                                  selected_index.indexrelid, 2, false
                              ) = 'role'
                              AND pg_get_indexdef(
                                  selected_index.indexrelid, 3, false
                              ) = 'sent_at'
                              AND pg_get_indexdef(selected_index.indexrelid)
                                  LIKE '% USING btree (task_id, role, sent_at DESC)'
                              AND selected_index.indoption[0] = 0
                              AND selected_index.indoption[1] = 0
                              AND selected_index.indoption[2] = 3
                          )
                      )
                ) AS id_index_conformant,
                (SELECT i.inhdetachpending FROM pg_inherits AS i
                 WHERE i.inhparent = to_regclass($2)
                   AND i.inhrelid = to_regclass($1)) AS detach_pending",
    )
    .bind(leaf_name)
    .bind(parent_name)
    .bind(id_index_name)
    .bind(expected_lower)
    .bind(expected_upper)
    .bind(heartbeat_index)
    .fetch_one(&mut *connection)
    .await;
    let restored = restore_timezone(connection, &prior).await;
    let row = result?;
    restored?;
    Ok(LeafPhysicalState {
        relation_exists: row.leaf_exists,
        parent_exists: row.parent_exists,
        partition_bound: row.partition_bound,
        partition_bound_matches_expected: row.partition_bound_matches_expected,
        id_index_conformant: row.id_index_conformant,
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
               AND am.amname = 'btree'
               AND i.indisvalid
               AND i.indisready
               AND i.indislive
               AND NOT i.indisunique
               AND NOT i.indisprimary
               AND NOT i.indisexclusion
               AND i.indpred IS NULL
               AND i.indexprs IS NULL
               AND i.indnkeyatts = 1
               AND i.indnatts = 1
               AND i.indkey[0] = (
                   SELECT a.attnum FROM pg_attribute AS a
                   WHERE a.attrelid = to_regclass($1) AND a.attname = $2
               )
               AND i.indcollation[0] = (
                   SELECT a.attcollation FROM pg_attribute AS a
                   WHERE a.attrelid = to_regclass($1) AND a.attname = $2
               )
               AND pg_get_indexdef(i.indexrelid, 1, false) = $2
               AND pg_get_indexdef(i.indexrelid)
                   LIKE '% USING btree (enqueued_at)'
               AND i.indoption[0] = 0
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

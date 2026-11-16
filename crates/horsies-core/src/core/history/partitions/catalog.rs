//! Fail-closed reads of the durable leaf catalog and PostgreSQL catalogs.

use chrono::{DateTime, Duration, Utc};
use sqlx::{FromRow, PgConnection};

use crate::core::history::commands::{is_safe_identifier, LeafBounds, OPEN_END_ANCHOR_SQL};
use crate::core::history::ddl::classes::{finite_class_parent_name, FOREVER_CLASS_KEY};
use crate::core::history::ddl::runtime_names::leaf_id_index_name;
use crate::core::history::errors::HistoryError;
use crate::core::history::heartbeats::partitioning::probe_index_name;
use crate::core::history::names::{
    HEARTBEATS_TABLE, HEARTBEAT_CLASS_KEY, LEAF_CATALOG, RETENTION_CLASSES, TASK_HISTORY_FOREVER,
    TASK_HISTORY_PARENT,
};
use crate::core::history::periods::{HeartbeatHorizonHours, RetentionDays};

pub const INDEX_SCHEMA_VERSION: i16 = 1;
pub const ORDERING_INDEX_COLUMN: &str = "enqueued_at";

/// What one `horsies_retention_classes` row is, by `class_kind`. Each kind
/// carries only its own fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionClassKind {
    /// `forever`: one static leaf, never dropped.
    Forever,
    /// `standard_<days>d`: daily leaves, dropped `days` after their upper bound.
    Period(RetentionDays),
    /// `heartbeats`: hourly heartbeat leaves, dropped `horizon` after their
    /// upper bound.
    Heartbeat(HeartbeatHorizonHours),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionClassRow {
    pub class_key: String,
    pub kind: RetentionClassKind,
}

impl RetentionClassRow {
    /// The relation that holds the leaves of this class. Derived from the
    /// kind and the key; the table stores no parent name.
    pub fn parent_name(&self) -> Result<String, HistoryError> {
        match self.kind {
            RetentionClassKind::Forever => Ok(TASK_HISTORY_FOREVER.to_owned()),
            RetentionClassKind::Period(_) => finite_class_parent_name(&self.class_key),
            RetentionClassKind::Heartbeat(_) => Ok(HEARTBEATS_TABLE.to_owned()),
        }
    }

    /// How long a leaf stays after its upper bound; `None` for `forever`.
    pub fn retention(&self) -> Option<Duration> {
        match self.kind {
            RetentionClassKind::Forever => None,
            RetentionClassKind::Period(days) => Some(days.as_duration()),
            RetentionClassKind::Heartbeat(horizon) => Some(horizon.as_duration()),
        }
    }
}

/// `lower_bound_kind` of a leaf whose partition starts at `MINVALUE`.
pub const OPEN_LOWER_BOUND_KIND: &str = "OPEN";
/// `lower_bound_kind` of a leaf whose partition starts at `lower_anchor`.
pub const AT_LOWER_BOUND_KIND: &str = "AT";

/// The lower bound of a cataloged leaf: the pair `(lower_bound_kind,
/// lower_anchor)` of the leaf catalog and the lookup manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LeafLowerBound {
    /// `OPEN` with no anchor: the partition starts at `MINVALUE`. Only the
    /// static `forever` leaf of the first migration has it. Sorts before every
    /// time, as `ORDER BY lower_anchor NULLS FIRST`.
    Open,
    /// `AT` with an anchor: every leaf that maintenance creates.
    At(DateTime<Utc>),
}

impl LeafLowerBound {
    /// The bound of a stored pair. The pair CHECKs of the catalog and the
    /// manifest allow only `OPEN` with no anchor and `AT` with one.
    pub fn decode(kind: &str, anchor: Option<DateTime<Utc>>) -> Result<Self, HistoryError> {
        match (kind, anchor) {
            (OPEN_LOWER_BOUND_KIND, None) => Ok(Self::Open),
            (AT_LOWER_BOUND_KIND, Some(anchor)) => Ok(Self::At(anchor)),
            (kind, anchor) => Err(HistoryError::contract(format!(
                "leaf lower bound kind {kind:?} with anchor {anchor:?} is not a stored pair"
            ))),
        }
    }

    /// The stored `lower_bound_kind`.
    pub fn kind(self) -> &'static str {
        match self {
            Self::Open => OPEN_LOWER_BOUND_KIND,
            Self::At(_) => AT_LOWER_BOUND_KIND,
        }
    }

    /// The stored `lower_anchor`: `None` for the open bound.
    pub fn at(self) -> Option<DateTime<Utc>> {
        match self {
            Self::Open => None,
            Self::At(value) => Some(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafCatalogRow {
    pub leaf_name: String,
    pub class_key: String,
    pub lower_anchor: LeafLowerBound,
    pub upper_anchor: DateTime<Utc>,
    pub index_schema_version: i16,
    pub created_at: DateTime<Utc>,
    pub detached_at: Option<DateTime<Utc>>,
    pub dropped_at: Option<DateTime<Utc>>,
}

/// The parent relation of a leaf of `class_key`: `horsies_heartbeats` for the
/// heartbeat class, else `horsies_task_history_<class_key>`.
pub fn leaf_parent_name(class_key: &str) -> Result<String, HistoryError> {
    match class_key {
        HEARTBEAT_CLASS_KEY => Ok(HEARTBEATS_TABLE.to_owned()),
        _ => finite_class_parent_name(class_key),
    }
}

/// The name of a leaf's identity index: `<leaf>_probe_idx` for a heartbeat
/// leaf, else `<leaf>_task_idx`.
pub fn leaf_identity_index_name(class_key: &str, leaf_name: &str) -> Result<String, HistoryError> {
    match class_key {
        HEARTBEAT_CLASS_KEY => probe_index_name(leaf_name),
        _ => Ok(leaf_id_index_name(leaf_name)),
    }
}

impl LeafCatalogRow {
    /// The parent relation, derived from the class key.
    pub fn parent_name(&self) -> Result<String, HistoryError> {
        leaf_parent_name(&self.class_key)
    }

    /// The identity index name, derived from the class key and leaf name.
    pub fn id_index_name(&self) -> Result<String, HistoryError> {
        leaf_identity_index_name(&self.class_key, &self.leaf_name)
    }

    /// Whether the row describes the requested class and bounds.
    pub fn describes(&self, class_key: &str, bounds: &LeafBounds) -> bool {
        self.class_key == class_key
            && self.lower_anchor == LeafLowerBound::At(bounds.lower())
            && self.upper_anchor == bounds.upper()
    }

    fn validate(self) -> Result<Self, HistoryError> {
        if !is_safe_identifier(&self.leaf_name) {
            return Err(HistoryError::contract(format!(
                "cataloged leaf name is not a safe identifier: {:?}",
                self.leaf_name
            )));
        }
        let ordered = match self.lower_anchor {
            LeafLowerBound::Open => true,
            LeafLowerBound::At(lower) => lower < self.upper_anchor,
        };
        if self.class_key.is_empty() || !ordered {
            return Err(HistoryError::contract(
                "leaf catalog row carries an invalid class or bounds",
            ));
        }
        self.parent_name()?;
        self.id_index_name()?;
        Ok(self)
    }
}

#[derive(FromRow)]
struct LeafCatalogRaw {
    leaf_name: String,
    class_key: String,
    lower_bound_kind: String,
    lower_anchor: Option<DateTime<Utc>>,
    upper_anchor: DateTime<Utc>,
    index_schema_version: i16,
    created_at: DateTime<Utc>,
    detached_at: Option<DateTime<Utc>>,
    dropped_at: Option<DateTime<Utc>>,
}

impl LeafCatalogRaw {
    fn into_row(self) -> Result<LeafCatalogRow, HistoryError> {
        LeafCatalogRow {
            lower_anchor: LeafLowerBound::decode(&self.lower_bound_kind, self.lower_anchor)?,
            leaf_name: self.leaf_name,
            class_key: self.class_key,
            upper_anchor: self.upper_anchor,
            index_schema_version: self.index_schema_version,
            created_at: self.created_at,
            detached_at: self.detached_at,
            dropped_at: self.dropped_at,
        }
        .validate()
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
    class_kind: String,
    retention_days: Option<i16>,
    heartbeat_horizon_hours: Option<i16>,
}

fn decode_retention_class(row: RetentionClassRaw) -> Result<RetentionClassRow, HistoryError> {
    let kind = match (
        row.class_kind.as_str(),
        row.retention_days,
        row.heartbeat_horizon_hours,
    ) {
        ("FOREVER", None, None) if row.class_key == FOREVER_CLASS_KEY => {
            RetentionClassKind::Forever
        }
        ("PERIOD", Some(days), None) => RetentionDays::from_stored(days)
            .filter(|days| days.class_key() == row.class_key)
            .map(RetentionClassKind::Period)
            .ok_or_else(|| {
                HistoryError::contract(format!(
                    "period class {:?} stores {days} days, which do not form its key",
                    row.class_key
                ))
            })?,
        ("HEARTBEAT", None, Some(hours)) if row.class_key == HEARTBEAT_CLASS_KEY => {
            HeartbeatHorizonHours::from_stored(hours)
                .map(RetentionClassKind::Heartbeat)
                .ok_or_else(|| {
                    HistoryError::contract(format!(
                        "heartbeat class stores a horizon of {hours} hours"
                    ))
                })?
        }
        (kind, _, _) => {
            return Err(HistoryError::contract(format!(
                "retention class {:?} has kind {kind:?} with the wrong shape",
                row.class_key
            )));
        }
    };
    Ok(RetentionClassRow {
        class_key: row.class_key,
        kind,
    })
}

pub async fn read_retention_class(
    connection: &mut PgConnection,
    class_key: &str,
) -> Result<Option<RetentionClassRow>, HistoryError> {
    let sql = format!(
        "SELECT class_key, class_kind, retention_days, heartbeat_horizon_hours
         FROM {RETENTION_CLASSES}
         WHERE class_key = $1"
    );
    let row: Option<RetentionClassRaw> = sqlx::query_as(&sql)
        .bind(class_key)
        .fetch_optional(connection)
        .await?;
    row.map(decode_retention_class).transpose()
}

/// The catalog columns that `LeafCatalogRaw` reads.
fn leaf_columns(alias: &str) -> String {
    format!(
        "{alias}.leaf_name, {alias}.class_key, {alias}.lower_bound_kind, {alias}.lower_anchor, \
         {alias}.upper_anchor, {alias}.index_schema_version, {alias}.created_at, \
         {alias}.detached_at, {alias}.dropped_at"
    )
}

/// Catalog predicate of the open-ended `forever` leaf: attached, open end.
pub(crate) fn open_ended_forever_leaf_predicate(alias: &str) -> String {
    format!(
        "{alias}.class_key = '{FOREVER_CLASS_KEY}' AND {alias}.upper_anchor = {OPEN_END_ANCHOR_SQL} \
         AND {alias}.detached_at IS NULL AND {alias}.dropped_at IS NULL"
    )
}

/// The catalog row of the attached open-ended `forever` leaf, when one exists.
pub async fn read_open_ended_forever_leaf(
    connection: &mut PgConnection,
) -> Result<Option<LeafCatalogRow>, HistoryError> {
    let sql = format!(
        "SELECT {} FROM {LEAF_CATALOG} AS catalog WHERE {} \
         ORDER BY catalog.lower_anchor NULLS FIRST LIMIT 1",
        leaf_columns("catalog"),
        open_ended_forever_leaf_predicate("catalog")
    );
    sqlx::query_as::<_, LeafCatalogRaw>(&sql)
        .fetch_optional(connection)
        .await?
        .map(LeafCatalogRaw::into_row)
        .transpose()
}

pub async fn read_leaf_catalog_row(
    connection: &mut PgConnection,
    leaf_name: &str,
) -> Result<Option<LeafCatalogRow>, HistoryError> {
    let sql = format!(
        "SELECT {} FROM {LEAF_CATALOG} AS catalog WHERE catalog.leaf_name = $1",
        leaf_columns("catalog")
    );
    sqlx::query_as::<_, LeafCatalogRaw>(&sql)
        .bind(leaf_name)
        .fetch_optional(connection)
        .await?
        .map(LeafCatalogRaw::into_row)
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
    let class_filter = match class_key {
        Some(_) => "AND catalog.class_key = $1",
        None => "",
    };
    let sql = format!(
        "SELECT {} FROM {LEAF_CATALOG} AS catalog
         WHERE catalog.detached_at IS NULL AND catalog.dropped_at IS NULL {class_filter}
         ORDER BY catalog.lower_anchor NULLS FIRST, catalog.leaf_name",
        leaf_columns("catalog")
    );
    let rows = match class_key {
        Some(class_key) => {
            sqlx::query_as::<_, LeafCatalogRaw>(&sql)
                .bind(class_key)
                .fetch_all(connection)
                .await?
        }
        None => {
            sqlx::query_as::<_, LeafCatalogRaw>(&sql)
                .fetch_all(connection)
                .await?
        }
    };
    rows.into_iter().map(LeafCatalogRaw::into_row).collect()
}

#[derive(FromRow)]
struct ManifestRaw {
    #[sqlx(flatten)]
    catalog: LeafCatalogRaw,
    relation_exists: bool,
    parent_exists: bool,
}

/// Every attached catalog row, with whether its relation and its derived
/// parent exist. `to_regclass` of a derived name: the parent is
/// `horsies_heartbeats` for the heartbeat class, else
/// `horsies_task_history_<class_key>`.
pub async fn read_manifest_leaf_rows(
    connection: &mut PgConnection,
) -> Result<ManifestLeafSelection, HistoryError> {
    let sql = format!(
        "SELECT {},
                to_regclass(catalog.leaf_name) IS NOT NULL AS relation_exists,
                to_regclass(
                    CASE WHEN catalog.class_key = $1 THEN $2
                         ELSE '{TASK_HISTORY_PARENT}_' || catalog.class_key
                    END
                ) IS NOT NULL AS parent_exists
         FROM {LEAF_CATALOG} AS catalog
         WHERE catalog.detached_at IS NULL AND catalog.dropped_at IS NULL
         ORDER BY catalog.lower_anchor NULLS FIRST, catalog.leaf_name",
        leaf_columns("catalog")
    );
    let rows: Vec<ManifestRaw> = sqlx::query_as(&sql)
        .bind(HEARTBEAT_CLASS_KEY)
        .bind(HEARTBEATS_TABLE)
        .fetch_all(connection)
        .await?;
    let mut attached = Vec::with_capacity(rows.len());
    let mut absent = Vec::new();
    let mut history_rows = 0_usize;
    let mut any_history_parent = false;
    for row in rows {
        let relation_exists = row.relation_exists;
        let parent_exists = row.parent_exists;
        let catalog = row.catalog.into_row()?;
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

#[cfg(test)]
mod open_ended_leaf_sql_tests {
    use super::*;

    #[test]
    fn lower_bound_pairs_decode_closed() {
        let anchor = DateTime::from_timestamp(86_400, 0).unwrap();
        assert_eq!(
            LeafLowerBound::decode("OPEN", None).unwrap(),
            LeafLowerBound::Open
        );
        assert_eq!(
            LeafLowerBound::decode("AT", Some(anchor)).unwrap(),
            LeafLowerBound::At(anchor)
        );
        for (kind, stored) in [("OPEN", Some(anchor)), ("AT", None), ("MIN", None)] {
            assert!(
                LeafLowerBound::decode(kind, stored).is_err(),
                "{kind} {stored:?}"
            );
        }
        assert!(LeafLowerBound::Open < LeafLowerBound::At(anchor));
        assert_eq!(LeafLowerBound::Open.kind(), "OPEN");
        assert_eq!(LeafLowerBound::At(anchor).at(), Some(anchor));
    }

    #[test]
    fn open_ended_predicate_renders_one_pinned_text() {
        assert_eq!(
            open_ended_forever_leaf_predicate("c"),
            "c.class_key = 'forever' AND c.upper_anchor = TIMESTAMPTZ '9999-01-01 00:00:00+00' \
             AND c.detached_at IS NULL AND c.dropped_at IS NULL"
        );
    }
}

//! Idempotent schema-v35 forever-class range conversion.

use chrono::{DateTime, Duration, Timelike, Utc};
use sqlx::PgConnection;

use crate::core::history::commands::{CreateDailyHistoryLeaf, LeafBounds, LeafRef};
use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;
use crate::core::history::ddl::runtime_names::{
    daily_leaf_name, leaf_enqueued_index_name, leaf_id_index_name,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{LEAF_CATALOG, TASK_HISTORY_FOREVER, TASK_HISTORY_PARENT};
use crate::core::history::outcomes::LeafCreation;

use super::catalog::{capture_partition_bound_utc, database_now, INDEX_SCHEMA_VERSION};
use super::manager::create_daily_leaf;
use super::publication::UnpublishedLoader;

pub const FOREVER_LEGACY_LEAF: &str = "horsies_task_history_forever_before_v35";

pub async fn ensure_forever_range_partitioning(
    connection: &mut PgConnection,
) -> Result<u64, HistoryError> {
    let relkind: Option<String> =
        sqlx::query_scalar("SELECT relkind::text FROM pg_class WHERE oid = to_regclass($1)")
            .bind(TASK_HISTORY_FOREVER)
            .fetch_optional(&mut *connection)
            .await?;
    let relkind = relkind
        .ok_or_else(|| HistoryError::contract(format!("{TASK_HISTORY_FOREVER} does not exist")))?;
    if relkind != "r" && relkind != "p" {
        return Err(HistoryError::contract(format!(
            "{TASK_HISTORY_FOREVER} must be a table or partitioned table, found {relkind:?}"
        )));
    }
    let now = database_now(connection).await?;
    let today = now
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(|| HistoryError::contract("database timestamp cannot be truncated"))?;
    let moved = if relkind == "r" {
        convert_unbounded_leaf(connection, today).await?
    } else {
        0
    };
    let leaf_name = daily_leaf_name(TASK_HISTORY_FOREVER, today)
        .map_err(|error| HistoryError::contract(error.to_string()))?;
    let leaf = LeafRef::new(
        leaf_name,
        FOREVER_CLASS_KEY,
        LeafBounds::new(today, today + Duration::days(1))
            .map_err(|error| HistoryError::contract(error.to_string()))?,
    )
    .map_err(|error| HistoryError::contract(error.to_string()))?;
    let command = CreateDailyHistoryLeaf::new(leaf)
        .map_err(|error| HistoryError::contract(error.to_string()))?;
    match create_daily_leaf(connection, &command, &UnpublishedLoader).await? {
        LeafCreation::Created { .. }
        | LeafCreation::AlreadyConformant { .. }
        | LeafCreation::IndexRepaired { .. } => Ok(moved),
        outcome => Err(HistoryError::contract(format!(
            "current forever leaf could not be ensured: {outcome:?}"
        ))),
    }
}

async fn convert_unbounded_leaf(
    connection: &mut PgConnection,
    today: DateTime<Utc>,
) -> Result<u64, HistoryError> {
    let legacy_id = leaf_id_index_name(FOREVER_LEGACY_LEAF);
    let legacy_order = leaf_enqueued_index_name(FOREVER_LEGACY_LEAF);
    let today_leaf = daily_leaf_name(TASK_HISTORY_FOREVER, today)
        .map_err(|error| HistoryError::contract(error.to_string()))?;
    for identifier in [FOREVER_LEGACY_LEAF, &legacy_id, &legacy_order, &today_leaf] {
        if identifier.len() > 63 {
            return Err(HistoryError::contract(
                "forever conversion identifier exceeds PostgreSQL limit",
            ));
        }
    }
    sqlx::query(&format!(
        "ALTER TABLE {TASK_HISTORY_PARENT} DETACH PARTITION {TASK_HISTORY_FOREVER}"
    ))
    .execute(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "DROP INDEX IF EXISTS {TASK_HISTORY_FOREVER}_task_idx"
    ))
    .execute(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "DROP INDEX IF EXISTS {TASK_HISTORY_FOREVER}_enqueued_idx"
    ))
    .execute(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "ALTER TABLE {TASK_HISTORY_FOREVER} RENAME TO {FOREVER_LEGACY_LEAF}"
    ))
    .execute(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "CREATE TABLE {TASK_HISTORY_FOREVER} PARTITION OF {TASK_HISTORY_PARENT}
         FOR VALUES IN ('{FOREVER_CLASS_KEY}') PARTITION BY RANGE (retention_anchor_at)"
    ))
    .execute(&mut *connection)
    .await?;
    let current = LeafRef::new(
        today_leaf,
        FOREVER_CLASS_KEY,
        LeafBounds::new(today, today + Duration::days(1))
            .map_err(|error| HistoryError::contract(error.to_string()))?,
    )
    .map_err(|error| HistoryError::contract(error.to_string()))?;
    let create = CreateDailyHistoryLeaf::new(current)
        .map_err(|error| HistoryError::contract(error.to_string()))?;
    if !matches!(
        create_daily_leaf(connection, &create, &UnpublishedLoader).await?,
        LeafCreation::Created { .. }
    ) {
        return Err(HistoryError::contract(
            "forever conversion could not create current leaf",
        ));
    }
    let moved: i64 = sqlx::query_scalar(&format!(
        "WITH moved AS (
             DELETE FROM {FOREVER_LEGACY_LEAF} WHERE retention_anchor_at >= $1 RETURNING *
         ), inserted AS (
             INSERT INTO {TASK_HISTORY_FOREVER} SELECT * FROM moved RETURNING 1
         ) SELECT count(*) FROM inserted"
    ))
    .bind(today)
    .fetch_one(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "ALTER TABLE {FOREVER_LEGACY_LEAF}
         ADD CONSTRAINT {FOREVER_LEGACY_LEAF}_anchor_check
         CHECK (retention_anchor_at < '{}')",
        today.to_rfc3339()
    ))
    .execute(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "ALTER TABLE {TASK_HISTORY_FOREVER} ATTACH PARTITION {FOREVER_LEGACY_LEAF}
         FOR VALUES FROM (MINVALUE) TO ('{}')",
        today.to_rfc3339()
    ))
    .execute(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "CREATE INDEX {legacy_id} ON {FOREVER_LEGACY_LEAF} (task_id)"
    ))
    .execute(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "CREATE INDEX {legacy_order} ON {FOREVER_LEGACY_LEAF} (enqueued_at)"
    ))
    .execute(&mut *connection)
    .await?;
    let bound = capture_partition_bound_utc(connection, FOREVER_LEGACY_LEAF)
        .await?
        .ok_or_else(|| HistoryError::contract("legacy forever leaf has no bound"))?;
    let lower = DateTime::from_timestamp(0, 0)
        .ok_or_else(|| HistoryError::contract("Unix epoch is not representable"))?;
    let sql = format!(
        "INSERT INTO {LEAF_CATALOG} (
             leaf_name, parent_name, class_key, lower_anchor, upper_anchor,
             index_schema_version, id_index_name, partition_bound,
             min_birth_at, min_birth_verified, created_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                   NULL, FALSE, statement_timestamp())"
    );
    sqlx::query(&sql)
        .bind(FOREVER_LEGACY_LEAF)
        .bind(TASK_HISTORY_FOREVER)
        .bind(FOREVER_CLASS_KEY)
        .bind(lower)
        .bind(today)
        .bind(INDEX_SCHEMA_VERSION)
        .bind(legacy_id)
        .bind(bound)
        .execute(&mut *connection)
        .await?;
    sqlx::query(&format!("ANALYZE {FOREVER_LEGACY_LEAF}"))
        .execute(connection)
        .await?;
    u64::try_from(moved).map_err(|_| HistoryError::contract("negative moved-row count"))
}

//! Hourly heartbeat registration, create-ahead, and expiry sweep.

use chrono::{DateTime, Duration, Timelike, Utc};
use sqlx::{PgConnection, PgPool};

use crate::core::history::commands::{
    is_safe_identifier, DetachExpiredHistoryLeaf, DropDetachedHistoryLeaf, InspectHistoryLeaf,
    LeafBounds, LeafRef, DETACH_STATEMENT_TIMEOUT_MS,
};
use crate::core::history::ddl::runtime_names::render_daily_leaf_ddl;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{
    HEARTBEATS_TABLE, HEARTBEAT_CLASS_KEY, LEAF_CATALOG, RETENTION_CLASSES,
};
use crate::core::history::outcomes::{CatalogConflictKind, LeafCreation, LeafDrop, LeafInspection};
use crate::core::history::partitions::catalog::{
    capture_partition_bound_utc, database_now, read_leaf_catalog_row, read_leaf_physical_state,
    read_retention_class, INDEX_SCHEMA_VERSION,
};
use crate::core::history::partitions::locks::lock_leaf_for_transaction;
use crate::core::history::partitions::manager::{
    detach_expired_leaf, drop_detached_leaf, inspect_leaf, DetachExpiredLeafOutcome, NoQuarantine,
};
use crate::core::history::partitions::publication::LoaderPublication;

const HOURLY: Duration = Duration::hours(1);
const HORIZON_FLOOR: Duration = Duration::hours(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatHorizonError {
    NonPositiveThreshold,
    InvalidSafetyFactor,
    Overflow,
}

pub fn heartbeat_horizon(
    stale_after: Duration,
    finalizing_stale_after: Duration,
    safety_factor: u32,
) -> Result<Duration, HeartbeatHorizonError> {
    if stale_after <= Duration::zero() || finalizing_stale_after <= Duration::zero() {
        return Err(HeartbeatHorizonError::NonPositiveThreshold);
    }
    if safety_factor < 1 {
        return Err(HeartbeatHorizonError::InvalidSafetyFactor);
    }
    let base = stale_after.max(finalizing_stale_after);
    let derived = base
        .checked_mul(i32::try_from(safety_factor).map_err(|_| HeartbeatHorizonError::Overflow)?)
        .ok_or(HeartbeatHorizonError::Overflow)?;
    Ok(derived.max(HORIZON_FLOOR))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatClassRegistration {
    Registered {
        horizon: Duration,
    },
    Verified {
        horizon: Duration,
    },
    HorizonUpdated {
        previous_horizon: Duration,
        horizon: Duration,
    },
    ParentUnpartitioned,
}

pub async fn register_heartbeat_class(
    connection: &mut PgConnection,
    horizon: Duration,
) -> Result<HeartbeatClassRegistration, HistoryError> {
    if horizon < HORIZON_FLOOR {
        return Err(HistoryError::contract(
            "heartbeat horizon is floored at one hour",
        ));
    }
    let partitioned: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pg_partitioned_table WHERE partrelid = to_regclass($1)
         )",
    )
    .bind(HEARTBEATS_TABLE)
    .fetch_one(&mut *connection)
    .await?;
    if !partitioned {
        return Ok(HeartbeatClassRegistration::ParentUnpartitioned);
    }
    match read_retention_class(connection, HEARTBEAT_CLASS_KEY).await? {
        None => {
            let duration_us = horizon
                .num_microseconds()
                .ok_or_else(|| HistoryError::contract("heartbeat horizon is out of range"))?;
            let sql = format!(
                "INSERT INTO {RETENTION_CLASSES} (
                     class_key, duration, partition_interval, finite_parent_name, created_at
                 ) VALUES ($1, $2::bigint * interval '1 microsecond', interval '1 hour',
                           $3, statement_timestamp())"
            );
            sqlx::query(&sql)
                .bind(HEARTBEAT_CLASS_KEY)
                .bind(duration_us)
                .bind(HEARTBEATS_TABLE)
                .execute(connection)
                .await?;
            Ok(HeartbeatClassRegistration::Registered { horizon })
        }
        Some(existing)
            if existing.partition_interval == Some(HOURLY)
                && existing.finite_parent_name.as_deref() == Some(HEARTBEATS_TABLE) =>
        {
            let stored = existing
                .duration
                .ok_or_else(|| HistoryError::contract("heartbeat class has no horizon"))?;
            if stored == horizon {
                return Ok(HeartbeatClassRegistration::Verified { horizon });
            }
            let duration_us = horizon
                .num_microseconds()
                .ok_or_else(|| HistoryError::contract("heartbeat horizon is out of range"))?;
            let sql = format!(
                "UPDATE {RETENTION_CLASSES}
                 SET duration = $1::bigint * interval '1 microsecond' WHERE class_key = $2"
            );
            sqlx::query(&sql)
                .bind(duration_us)
                .bind(HEARTBEAT_CLASS_KEY)
                .execute(connection)
                .await?;
            Ok(HeartbeatClassRegistration::HorizonUpdated {
                previous_horizon: stored,
                horizon,
            })
        }
        Some(_) => Err(HistoryError::contract(
            "heartbeat class row carries a non-heartbeat shape",
        )),
    }
}

pub fn hourly_leaf_name(lower: DateTime<Utc>) -> Result<String, HistoryError> {
    let name = format!("{HEARTBEATS_TABLE}_{}", lower.format("%Y_%m_%d_%H"));
    if is_safe_identifier(&name) {
        Ok(name)
    } else {
        Err(HistoryError::contract("unsafe hourly heartbeat leaf name"))
    }
}

pub fn probe_index_name(leaf_name: &str) -> Result<String, HistoryError> {
    let name = format!("{leaf_name}_probe_idx");
    if is_safe_identifier(&name) {
        Ok(name)
    } else {
        Err(HistoryError::contract("unsafe heartbeat probe index name"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateHourlyHeartbeatLeaf {
    leaf: LeafRef,
}

impl CreateHourlyHeartbeatLeaf {
    pub fn new(leaf: LeafRef) -> Result<Self, HistoryError> {
        if leaf.class_key() != HEARTBEAT_CLASS_KEY {
            return Err(HistoryError::contract(
                "heartbeat leaf must carry the reserved class key",
            ));
        }
        if leaf.bounds().upper() - leaf.bounds().lower() != HOURLY {
            return Err(HistoryError::contract(
                "heartbeat leaf bounds must span exactly one hour",
            ));
        }
        Ok(Self { leaf })
    }

    pub fn leaf(&self) -> &LeafRef {
        &self.leaf
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnsureHeartbeatCoverage {
    horizon_hours: u32,
}

impl EnsureHeartbeatCoverage {
    pub fn new(horizon_hours: u32) -> Result<Self, HistoryError> {
        if horizon_hours < 2 {
            return Err(HistoryError::contract(
                "heartbeat coverage must include at least two future leaves",
            ));
        }
        Ok(Self { horizon_hours })
    }

    pub fn horizon_hours(&self) -> u32 {
        self.horizon_hours
    }
}

pub fn hourly_leaf_ref(lower: DateTime<Utc>) -> Result<LeafRef, HistoryError> {
    LeafRef::new(
        hourly_leaf_name(lower)?,
        HEARTBEAT_CLASS_KEY,
        LeafBounds::new(lower, lower + HOURLY)
            .map_err(|error| HistoryError::contract(error.to_string()))?,
    )
    .map_err(|error| HistoryError::contract(error.to_string()))
}

pub async fn create_hourly_heartbeat_leaf(
    connection: &mut PgConnection,
    command: &CreateHourlyHeartbeatLeaf,
) -> Result<LeafCreation, HistoryError> {
    let leaf = command.leaf();
    if read_retention_class(connection, leaf.class_key())
        .await?
        .is_none()
    {
        return Ok(LeafCreation::RetentionClassAbsent {
            class_key: leaf.class_key().to_owned(),
        });
    }
    lock_leaf_for_transaction(connection, leaf.class_key(), leaf.bounds().lower()).await?;
    let index_name = probe_index_name(leaf.leaf_name())?;
    let catalog = read_leaf_catalog_row(connection, leaf.leaf_name()).await?;
    let physical = read_leaf_physical_state(
        connection,
        leaf.leaf_name(),
        HEARTBEATS_TABLE,
        catalog
            .as_ref()
            .map_or(index_name.as_str(), |row| row.id_index_name.as_str()),
    )
    .await?;
    if physical.relation_exists != catalog.is_some() {
        return Ok(LeafCreation::CatalogConflict {
            leaf_name: leaf.leaf_name().to_owned(),
            kind: CatalogConflictKind::RelationWithoutCatalog,
            detail: if physical.relation_exists {
                "relation exists without a catalog row".to_owned()
            } else {
                "catalog row exists without a relation".to_owned()
            },
        });
    }
    if let Some(catalog) = catalog {
        if catalog.parent_name != HEARTBEATS_TABLE
            || catalog.class_key != leaf.class_key()
            || catalog.lower_anchor != leaf.bounds().lower()
            || catalog.upper_anchor != leaf.bounds().upper()
            || catalog.dropped_at.is_some()
        {
            return Ok(LeafCreation::CatalogConflict {
                leaf_name: leaf.leaf_name().to_owned(),
                kind: CatalogConflictKind::MetadataMismatch,
                detail: "existing leaf metadata differs from the request".to_owned(),
            });
        }
        if physical.partition_bound.as_deref() != Some(catalog.partition_bound.as_str()) {
            return Ok(LeafCreation::CatalogConflict {
                leaf_name: leaf.leaf_name().to_owned(),
                kind: CatalogConflictKind::PhysicalNonconformant,
                detail: "attached leaf partition bound differs from catalog".to_owned(),
            });
        }
        if !physical.id_index_exists {
            sqlx::query(&format!(
                "CREATE INDEX {} ON {} (task_id, role, sent_at DESC)",
                catalog.id_index_name,
                leaf.leaf_name()
            ))
            .execute(connection)
            .await?;
            return Ok(LeafCreation::IndexRepaired {
                leaf_name: leaf.leaf_name().to_owned(),
                id_index_name: catalog.id_index_name,
            });
        }
        return Ok(LeafCreation::AlreadyConformant {
            leaf_name: leaf.leaf_name().to_owned(),
        });
    }
    sqlx::query(&render_daily_leaf_ddl(HEARTBEATS_TABLE, leaf)?)
        .execute(&mut *connection)
        .await?;
    let bound = capture_partition_bound_utc(connection, leaf.leaf_name())
        .await?
        .ok_or_else(|| HistoryError::contract("new heartbeat partition has no bound"))?;
    let sql = format!(
        "INSERT INTO {LEAF_CATALOG} (
             leaf_name, parent_name, class_key, lower_anchor, upper_anchor,
             index_schema_version, id_index_name, partition_bound,
             min_birth_at, min_birth_verified, created_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                   NULL, TRUE, statement_timestamp())"
    );
    sqlx::query(&sql)
        .bind(leaf.leaf_name())
        .bind(HEARTBEATS_TABLE)
        .bind(leaf.class_key())
        .bind(leaf.bounds().lower())
        .bind(leaf.bounds().upper())
        .bind(INDEX_SCHEMA_VERSION)
        .bind(&index_name)
        .bind(bound)
        .execute(&mut *connection)
        .await?;
    sqlx::query(&format!(
        "CREATE INDEX {index_name} ON {} (task_id, role, sent_at DESC)",
        leaf.leaf_name()
    ))
    .execute(connection)
    .await?;
    Ok(LeafCreation::Created {
        leaf_name: leaf.leaf_name().to_owned(),
        id_index_name: index_name,
    })
}

pub async fn ensure_heartbeat_coverage(
    connection: &mut PgConnection,
    command: &EnsureHeartbeatCoverage,
) -> Result<Vec<LeafCreation>, HistoryError> {
    let now = database_now(connection).await?;
    let hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(|| HistoryError::contract("database timestamp cannot be truncated"))?;
    let mut outcomes = Vec::with_capacity(command.horizon_hours() as usize + 1);
    for offset in 0..=command.horizon_hours() {
        let leaf = hourly_leaf_ref(hour + Duration::hours(i64::from(offset)))?;
        let outcome =
            create_hourly_heartbeat_leaf(connection, &CreateHourlyHeartbeatLeaf::new(leaf)?)
                .await?;
        let keep_going = matches!(
            outcome,
            LeafCreation::Created { .. }
                | LeafCreation::AlreadyConformant { .. }
                | LeafCreation::IndexRepaired { .. }
        );
        outcomes.push(outcome);
        if !keep_going {
            break;
        }
    }
    Ok(outcomes)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatLeafSwept {
    pub leaf_name: String,
    pub detach: DetachExpiredLeafOutcome,
    pub drop: Option<LeafDrop>,
}

pub async fn sweep_expired_heartbeat_leaves<P: LoaderPublication>(
    pool: &PgPool,
    publisher: &P,
) -> Result<Vec<HeartbeatLeafSwept>, HistoryError> {
    let sql = format!(
        "SELECT c.leaf_name, c.lower_anchor, c.upper_anchor
         FROM {LEAF_CATALOG} AS c JOIN {RETENTION_CLASSES} AS r
           ON r.class_key = c.class_key
         WHERE c.class_key = $1 AND c.dropped_at IS NULL
           AND c.upper_anchor + r.duration <= statement_timestamp()
         ORDER BY c.lower_anchor"
    );
    let rows: Vec<(String, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(&sql)
        .bind(HEARTBEAT_CLASS_KEY)
        .fetch_all(pool)
        .await?;
    let mut swept = Vec::with_capacity(rows.len());
    for (leaf_name, lower, upper) in rows {
        let leaf = LeafRef::new(
            leaf_name,
            HEARTBEAT_CLASS_KEY,
            LeafBounds::new(lower, upper)
                .map_err(|error| HistoryError::contract(error.to_string()))?,
        )
        .map_err(|error| HistoryError::contract(error.to_string()))?;
        let detach_command =
            DetachExpiredHistoryLeaf::new(leaf.clone(), None, Some(DETACH_STATEMENT_TIMEOUT_MS))
                .map_err(|error| HistoryError::contract(error.to_string()))?;
        let detach = detach_expired_leaf(pool, &detach_command, publisher, &NoQuarantine).await?;
        let is_detached = matches!(
            &detach,
            DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. })
        );
        let detached = if is_detached {
            true
        } else {
            let mut connection = pool.acquire().await?;
            matches!(
                inspect_leaf(&mut connection, &InspectHistoryLeaf::new(leaf.clone())).await?,
                LeafInspection::Detached { .. }
            )
        };
        let drop = if detached {
            let mut transaction = pool.begin().await?;
            let outcome = drop_detached_leaf(
                &mut transaction,
                &DropDetachedHistoryLeaf::new(leaf.clone()),
                publisher,
            )
            .await?;
            transaction.commit().await?;
            Some(outcome)
        } else {
            None
        };
        swept.push(HeartbeatLeafSwept {
            leaf_name: leaf.leaf_name().to_owned(),
            detach,
            drop,
        });
    }
    Ok(swept)
}

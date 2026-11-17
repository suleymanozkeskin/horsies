//! Hourly heartbeat registration, create-ahead, and expiry sweep.

use chrono::{DateTime, Duration, Timelike, Utc};
use sqlx::PgConnection;

use crate::broker::core_rows::StatementLog;
use crate::core::history::commands::{is_safe_identifier, LeafBounds, LeafRef};
use crate::core::history::ddl::runtime_names::render_daily_leaf_ddl;
use crate::core::history::errors::HistoryError;
use crate::core::history::maintenance::pruning::{
    pass_candidates, sweep_counts_toward_bound, sweep_retention_leaf, LeafSweep, PruneReach,
};
use crate::core::history::names::{
    HEARTBEATS_TABLE, HEARTBEAT_CLASS_KEY, LEAF_CATALOG, RETENTION_CLASSES,
};
use crate::core::history::outcomes::{CatalogConflictKind, LeafCreation, LeafDrop};
use crate::core::history::partitions::catalog::{
    database_now, read_leaf_catalog_row, read_leaf_physical_state, read_retention_class,
    LeafIndexKind, LeafPartitionBoundExpectation, RetentionClassKind, AT_LOWER_BOUND_KIND,
    INDEX_SCHEMA_VERSION,
};
use crate::core::history::partitions::locks::{
    try_lock_leaf_for_transaction, try_lock_relation_exclusive_for_transaction, LeafLockAttempt,
};
use crate::core::history::partitions::manager::{
    remove_attached_index_for_repair, DetachExpiredLeafOutcome, IndexRepairRemoval,
};
use crate::core::history::partitions::retirement::RetiredClassKind;
use crate::core::history::periods::HeartbeatHorizonHours;
use crate::db::CorePool;

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

/// `horsies_retention_classes.class_kind` of the heartbeat class.
pub const HEARTBEAT_CLASS_KIND: &str = "HEARTBEAT";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatClassRegistration {
    Registered {
        horizon: HeartbeatHorizonHours,
    },
    Verified {
        horizon: HeartbeatHorizonHours,
    },
    HorizonUpdated {
        previous_horizon: HeartbeatHorizonHours,
        horizon: HeartbeatHorizonHours,
    },
    ParentUnpartitioned,
}

/// Create the heartbeat class row, verify it, or update its stored horizon,
/// inside the caller's transaction. The horizon is the only class value that
/// changes in place: configuration sets it at every registration. Does not
/// commit.
pub async fn register_heartbeat_class(
    connection: &mut PgConnection,
    horizon: HeartbeatHorizonHours,
) -> Result<HeartbeatClassRegistration, HistoryError> {
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
            let sql = format!(
                "INSERT INTO {RETENTION_CLASSES} (
                     class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at
                 ) VALUES ($1, '{HEARTBEAT_CLASS_KIND}', NULL, $2, statement_timestamp())"
            );
            sqlx::query(&sql)
                .bind(HEARTBEAT_CLASS_KEY)
                .bind(horizon.as_stored())
                .execute(connection)
                .await?;
            Ok(HeartbeatClassRegistration::Registered { horizon })
        }
        Some(existing) => match existing.kind {
            RetentionClassKind::Heartbeat(stored) if stored == horizon => {
                Ok(HeartbeatClassRegistration::Verified { horizon })
            }
            RetentionClassKind::Heartbeat(stored) => {
                let sql = format!(
                    "UPDATE {RETENTION_CLASSES}
                     SET heartbeat_horizon_hours = $1 WHERE class_key = $2"
                );
                sqlx::query(&sql)
                    .bind(horizon.as_stored())
                    .bind(HEARTBEAT_CLASS_KEY)
                    .execute(connection)
                    .await?;
                Ok(HeartbeatClassRegistration::HorizonUpdated {
                    previous_horizon: stored,
                    horizon,
                })
            }
            RetentionClassKind::Forever | RetentionClassKind::Period(_) => Err(
                HistoryError::contract("heartbeat class row carries a non-heartbeat shape"),
            ),
        },
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
    if heartbeat_leaf_is_conformant(connection, leaf).await? {
        return Ok(LeafCreation::AlreadyConformant {
            leaf_name: leaf.leaf_name().to_owned(),
        });
    }
    if matches!(
        try_lock_leaf_for_transaction(connection, leaf.class_key(), leaf.bounds().lower()).await?,
        LeafLockAttempt::Busy
    ) {
        return Ok(LeafCreation::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        });
    }
    let index_name = probe_index_name(leaf.leaf_name())?;
    let catalog = read_leaf_catalog_row(connection, leaf.leaf_name()).await?;
    let physical = read_leaf_physical_state(
        connection,
        leaf.leaf_name(),
        HEARTBEATS_TABLE,
        &index_name,
        LeafPartitionBoundExpectation::Requested(leaf.bounds()),
        LeafIndexKind::Heartbeat,
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
        if !catalog.describes(leaf.class_key(), leaf.bounds())
            || catalog.detached_at.is_some()
            || catalog.dropped_at.is_some()
        {
            return Ok(LeafCreation::CatalogConflict {
                leaf_name: leaf.leaf_name().to_owned(),
                kind: CatalogConflictKind::MetadataMismatch,
                detail: "existing leaf metadata differs from the request".to_owned(),
            });
        }
        if !physical.partition_bound_matches_expected {
            return Ok(LeafCreation::CatalogConflict {
                leaf_name: leaf.leaf_name().to_owned(),
                kind: CatalogConflictKind::PhysicalNonconformant,
                detail: "attached leaf partition bound differs from the requested bounds"
                    .to_owned(),
            });
        }
        if physical.detach_pending != Some(false) {
            return Ok(LeafCreation::CatalogConflict {
                leaf_name: leaf.leaf_name().to_owned(),
                kind: CatalogConflictKind::PhysicalNonconformant,
                detail: "required heartbeat leaf is not attached to its cataloged parent"
                    .to_owned(),
            });
        }
        if !physical.id_index_conformant {
            if matches!(
                try_lock_relation_exclusive_for_transaction(connection, leaf.leaf_name()).await?,
                LeafLockAttempt::Busy
            ) {
                return Ok(LeafCreation::Busy {
                    leaf_name: leaf.leaf_name().to_owned(),
                });
            }
            // Read the state again under the relation lock.
            let physical = read_leaf_physical_state(
                connection,
                leaf.leaf_name(),
                HEARTBEATS_TABLE,
                &index_name,
                LeafPartitionBoundExpectation::Requested(leaf.bounds()),
                LeafIndexKind::Heartbeat,
            )
            .await?;
            if !physical.relation_exists
                || physical.detach_pending != Some(false)
                || !physical.partition_bound_matches_expected
            {
                return Ok(LeafCreation::CatalogConflict {
                    leaf_name: leaf.leaf_name().to_owned(),
                    kind: CatalogConflictKind::PhysicalNonconformant,
                    detail: "the leaf changed before its index repair took the relation lock"
                        .to_owned(),
                });
            }
            if physical.id_index_conformant {
                return Ok(LeafCreation::AlreadyConformant {
                    leaf_name: leaf.leaf_name().to_owned(),
                });
            }
            match remove_attached_index_for_repair(connection, leaf.leaf_name(), &index_name)
                .await?
            {
                IndexRepairRemoval::Absent | IndexRepairRemoval::Removed => {}
                IndexRepairRemoval::Busy => {
                    return Ok(LeafCreation::Busy {
                        leaf_name: leaf.leaf_name().to_owned(),
                    });
                }
                IndexRepairRemoval::Foreign => {
                    return Ok(LeafCreation::CatalogConflict {
                        leaf_name: leaf.leaf_name().to_owned(),
                        kind: CatalogConflictKind::PhysicalNonconformant,
                        detail: "cataloged heartbeat index name belongs to another relation"
                            .to_owned(),
                    });
                }
            }
            sqlx::query(&format!(
                "CREATE INDEX {index_name} ON {} (task_id, role, sent_at DESC)",
                leaf.leaf_name()
            ))
            .execute(connection)
            .await?;
            return Ok(LeafCreation::IndexRepaired {
                leaf_name: leaf.leaf_name().to_owned(),
                id_index_name: index_name,
            });
        }
        return Ok(LeafCreation::AlreadyConformant {
            leaf_name: leaf.leaf_name().to_owned(),
        });
    }
    if matches!(
        try_lock_relation_exclusive_for_transaction(connection, HEARTBEATS_TABLE).await?,
        LeafLockAttempt::Busy
    ) {
        return Ok(LeafCreation::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        });
    }
    sqlx::query(&render_daily_leaf_ddl(HEARTBEATS_TABLE, leaf)?)
        .execute(&mut *connection)
        .await?;
    let sql = format!(
        "INSERT INTO {LEAF_CATALOG} (
             leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor,
             index_schema_version, created_at, detached_at, dropped_at
         ) VALUES ($1, $2, '{AT_LOWER_BOUND_KIND}', $3, $4, $5, statement_timestamp(), NULL, NULL)"
    );
    sqlx::query(&sql)
        .bind(leaf.leaf_name())
        .bind(leaf.class_key())
        .bind(leaf.bounds().lower())
        .bind(leaf.bounds().upper())
        .bind(INDEX_SCHEMA_VERSION)
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

async fn heartbeat_leaf_is_conformant(
    connection: &mut PgConnection,
    leaf: &LeafRef,
) -> Result<bool, HistoryError> {
    let Some(catalog) = read_leaf_catalog_row(connection, leaf.leaf_name()).await? else {
        return Ok(false);
    };
    if !catalog.describes(leaf.class_key(), leaf.bounds())
        || catalog.detached_at.is_some()
        || catalog.dropped_at.is_some()
    {
        return Ok(false);
    }
    let physical = read_leaf_physical_state(
        connection,
        leaf.leaf_name(),
        HEARTBEATS_TABLE,
        &probe_index_name(leaf.leaf_name())?,
        LeafPartitionBoundExpectation::Requested(leaf.bounds()),
        LeafIndexKind::Heartbeat,
    )
    .await?;
    Ok(physical.relation_exists
        && physical.id_index_conformant
        && physical.detach_pending == Some(false)
        && physical.partition_bound_matches_expected)
}

pub async fn ensure_heartbeat_coverage(
    connection: &mut PgConnection,
    command: &EnsureHeartbeatCoverage,
) -> Result<Vec<LeafCreation>, HistoryError> {
    let commands = plan_heartbeat_coverage(connection, command).await?;
    let mut outcomes = Vec::with_capacity(commands.len());
    for create in commands {
        let outcome = create_hourly_heartbeat_leaf(connection, &create).await?;
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

pub(crate) async fn plan_heartbeat_coverage(
    connection: &mut PgConnection,
    command: &EnsureHeartbeatCoverage,
) -> Result<Vec<CreateHourlyHeartbeatLeaf>, HistoryError> {
    let now = database_now(connection).await?;
    let hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(|| HistoryError::contract("database timestamp cannot be truncated"))?;
    let mut commands = Vec::with_capacity(command.horizon_hours() as usize + 1);
    for offset in 0..=command.horizon_hours() {
        let leaf = hourly_leaf_ref(hour + Duration::hours(i64::from(offset)))?;
        commands.push(CreateHourlyHeartbeatLeaf::new(leaf)?);
    }
    Ok(commands)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatLeafSwept {
    pub leaf_name: String,
    pub detach: DetachExpiredLeafOutcome,
    pub drop: Option<LeafDrop>,
}

/// Detach and drop the given expired heartbeat leaves, in the given order,
/// until `max_acted_leaves` leaves count toward the bound
/// (`sweep_counts_toward_bound`). Each leaf is swept as a period leaf is
/// (`pruning::sweep_retention_leaf`). A leaf whose sweep fails counts, since
/// its effects are unknown; its error goes to the error list and the sweep
/// goes on with the next leaf. `Truncated` when the bound left leaves for the
/// next pass.
pub(crate) async fn sweep_heartbeat_leaves(
    pool: &CorePool,
    log: StatementLog,
    leaves: &[LeafRef],
    max_acted_leaves: usize,
) -> LeafSweep<HeartbeatLeafSwept> {
    let mut swept = Vec::new();
    let mut errors = Vec::new();
    let mut acted = 0;
    let mut reach = PruneReach::WithinBounds;
    for leaf in leaves {
        if acted >= max_acted_leaves {
            reach = PruneReach::Truncated;
            break;
        }
        match sweep_retention_leaf(pool, log, leaf).await {
            Ok((detach, drop)) => {
                acted += usize::from(sweep_counts_toward_bound(&detach, drop.as_ref()));
                swept.push(HeartbeatLeafSwept {
                    leaf_name: leaf.leaf_name().to_owned(),
                    detach,
                    drop,
                });
            }
            Err(error) => {
                acted += 1;
                errors.push(format!("{}: {error}", leaf.leaf_name()));
            }
        }
    }
    LeafSweep {
        swept,
        errors,
        acted,
        reach,
    }
}

/// Sweep expired heartbeat leaves, oldest first, on a direct or
/// session-capable core pool: one candidate read (`horsies_prune_candidates`,
/// at most `MAX_PRUNE_CANDIDATES_PER_PASS` leaves), then
/// `sweep_heartbeat_leaves` over its heartbeat leaves. `Truncated` when the
/// candidate read or the leaf bound left expired leaves for the next pass.
///
/// Errors: the failed candidate read, which sweeps nothing.
pub(crate) async fn sweep_expired_heartbeat_leaves(
    pool: &CorePool,
    log: StatementLog,
    max_acted_leaves: usize,
) -> Result<LeafSweep<HeartbeatLeafSwept>, HistoryError> {
    let candidates = pass_candidates(pool, log).await?;
    let sweep = sweep_heartbeat_leaves(
        pool,
        log,
        &candidates.of_kind(RetiredClassKind::Heartbeat),
        max_acted_leaves,
    )
    .await;
    let reach = match (candidates.reach, sweep.reach) {
        (PruneReach::WithinBounds, PruneReach::WithinBounds) => PruneReach::WithinBounds,
        _ => PruneReach::Truncated,
    };
    Ok(LeafSweep { reach, ..sweep })
}

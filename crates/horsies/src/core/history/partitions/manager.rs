//! Typed lifecycle for one task-history leaf.

use std::future::Future;
use std::ops::{Deref, DerefMut};

use chrono::{Duration, Timelike};
use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres};
use uuid::Uuid;

use crate::core::history::commands::{
    is_safe_identifier, CreateDailyHistoryLeaf, DetachExpiredHistoryLeaf, DropDetachedHistoryLeaf,
    EnsureLeafCoverage, FinalizeInterruptedLeafDetach, InspectHistoryLeaf, LeafBounds, LeafRef,
};
use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;
use crate::core::history::ddl::runtime_names::{
    daily_leaf_name, leaf_enqueued_index_name, leaf_id_index_name, render_daily_leaf_ddl,
    render_leaf_enqueued_index_ddl, render_leaf_id_index_ddl,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{
    HEARTBEAT_CLASS_KEY, LEAF_CATALOG, TASK_HISTORY_FOREVER, WORKFLOW_PHASE2_PENDING,
};
use crate::core::history::outcomes::{
    CatalogConflictKind, LeafAttachment, LeafCreation, LeafDrop, LeafInspection,
};

use super::catalog::{
    capture_partition_bound_utc, database_now, read_leaf_catalog_row,
    read_leaf_ordering_index_exists, read_leaf_physical_state, read_retention_class, LeafIndexKind,
    LeafPartitionBoundExpectation, RetentionClassRow, INDEX_SCHEMA_VERSION,
};
use super::locks::{
    is_lock_not_available, try_lock_leaf_for_session, try_lock_leaf_for_transaction,
    try_lock_relation_exclusive_for_transaction, unlock_leaf_for_session, LeafLockAttempt,
};
use super::publication::LoaderPublication;

const DAILY: Duration = Duration::days(1);
const LEAF_DDL_LOCK_TIMEOUT_MS: u64 = 2_000;

pub(crate) enum DailyCoveragePlan {
    Leaves(Vec<CreateDailyHistoryLeaf>),
    Refused(LeafCreation),
}

/// A session-lock connection is reusable only after explicit cleanup.
/// Cancellation drops this guard before cleanup and closes the socket, which
/// makes PostgreSQL release the session advisory lock instead of returning a
/// poisoned connection to the pool.
struct SessionConnection {
    inner: PoolConnection<Postgres>,
    reusable: bool,
}

impl SessionConnection {
    fn new(inner: PoolConnection<Postgres>) -> Self {
        Self {
            inner,
            reusable: false,
        }
    }

    fn mark_reusable(&mut self) {
        self.reusable = true;
    }
}

impl Deref for SessionConnection {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl DerefMut for SessionConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut()
    }
}

impl Drop for SessionConnection {
    fn drop(&mut self) {
        if !self.reusable {
            self.inner.close_on_drop();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuarantineRefusalVerdict {
    NodeRowAbsent,
    NodeIdentityAbsent,
    SourceAbsent,
    CopyVerificationFailed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskQuarantineRefusal {
    pub task_id: Uuid,
    pub verdict: QuarantineRefusalVerdict,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineRefused {
    pub leaf_name: String,
    pub repointed: u64,
    pub refusals: Vec<TaskQuarantineRefusal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuarantineResult {
    NoOverHorizonBlockers {
        leaf_name: String,
    },
    BlockersQuarantined {
        leaf_name: String,
        repointed: u64,
        drained: u64,
    },
    Refused(QuarantineRefused),
}

pub trait LeafBlockerQuarantine: Send + Sync {
    fn quarantine(
        &self,
        connection: &mut PgConnection,
        leaf: &LeafRef,
        horizon: Duration,
    ) -> impl Future<Output = Result<QuarantineResult, HistoryError>> + Send;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoQuarantine;

impl LeafBlockerQuarantine for NoQuarantine {
    async fn quarantine(
        &self,
        _connection: &mut PgConnection,
        _leaf: &LeafRef,
        _horizon: Duration,
    ) -> Result<QuarantineResult, HistoryError> {
        Err(HistoryError::contract(
            "phase-2 quarantine provider is not installed",
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetachExpiredLeafOutcome {
    Busy { leaf_name: String },
    Inspection(LeafInspection),
    QuarantineRefused(QuarantineRefused),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalizeInterruptedLeafOutcome {
    Busy { leaf_name: String },
    Inspection(LeafInspection),
}

fn history_class_parent(class_key: &str, retention_class: &RetentionClassRow) -> Option<String> {
    if class_key == FOREVER_CLASS_KEY && retention_class.duration.is_none() {
        Some(TASK_HISTORY_FOREVER.to_owned())
    } else {
        retention_class.finite_parent_name.clone()
    }
}

fn interval_days(interval: Option<Duration>) -> Option<i64> {
    interval.map(|value| value.num_days())
}

pub async fn inspect_leaf(
    connection: &mut PgConnection,
    command: &InspectHistoryLeaf,
) -> Result<LeafInspection, HistoryError> {
    let leaf = command.leaf();
    let Some(retention_class) = read_retention_class(connection, leaf.class_key()).await? else {
        return Ok(LeafInspection::RetentionClassAbsent {
            class_key: leaf.class_key().to_owned(),
        });
    };
    let Some(duration) = retention_class.duration else {
        return Ok(LeafInspection::ForeverClassLeaf {
            class_key: leaf.class_key().to_owned(),
        });
    };
    let parent_name = retention_class
        .finite_parent_name
        .as_deref()
        .ok_or_else(|| HistoryError::contract("finite retention class has no physical parent"))?;
    let catalog = read_leaf_catalog_row(connection, leaf.leaf_name()).await?;
    let id_index_name = catalog.as_ref().map_or_else(
        || leaf_id_index_name(leaf.leaf_name()),
        |row| row.id_index_name.clone(),
    );
    let physical = read_leaf_physical_state(
        connection,
        leaf.leaf_name(),
        parent_name,
        &id_index_name,
        LeafPartitionBoundExpectation::Requested(leaf.bounds()),
        match leaf.class_key() {
            HEARTBEAT_CLASS_KEY => LeafIndexKind::Heartbeat,
            _ => LeafIndexKind::History,
        },
    )
    .await?;
    let expires_at = leaf.bounds().upper() + duration;

    let Some(catalog) = catalog else {
        if physical.relation_exists {
            return Ok(LeafInspection::CatalogConflict {
                leaf_name: leaf.leaf_name().to_owned(),
                kind: CatalogConflictKind::RelationWithoutCatalog,
                detail: "relation exists but the leaf catalog has no row for it".to_owned(),
            });
        }
        return Ok(LeafInspection::Missing {
            leaf_name: leaf.leaf_name().to_owned(),
            cataloged: false,
            expires_at: Some(expires_at),
        });
    };

    if catalog.parent_name != parent_name
        || catalog.class_key != leaf.class_key()
        || catalog.lower_anchor != leaf.bounds().lower()
        || catalog.upper_anchor != leaf.bounds().upper()
    {
        return Ok(LeafInspection::CatalogConflict {
            leaf_name: leaf.leaf_name().to_owned(),
            kind: CatalogConflictKind::MetadataMismatch,
            detail: "catalog row disagrees with the requested class or bounds".to_owned(),
        });
    }
    if !physical.relation_exists {
        return if catalog.dropped_at.is_some() {
            Ok(LeafInspection::Dropped {
                leaf_name: leaf.leaf_name().to_owned(),
            })
        } else {
            Ok(LeafInspection::Missing {
                leaf_name: leaf.leaf_name().to_owned(),
                cataloged: true,
                expires_at: Some(expires_at),
            })
        };
    }
    if !physical.parent_exists {
        return Err(HistoryError::HistoryParentAbsent(format!(
            "finite history parent {parent_name:?} does not exist"
        )));
    }
    if physical.detach_pending.is_some()
        && (physical.partition_bound.as_deref() != Some(catalog.partition_bound.as_str())
            || !physical.partition_bound_matches_expected
            || !physical.id_index_conformant)
    {
        return Ok(LeafInspection::CatalogConflict {
            leaf_name: leaf.leaf_name().to_owned(),
            kind: CatalogConflictKind::PhysicalNonconformant,
            detail: "attached leaf bound or task-ID index disagrees with catalog".to_owned(),
        });
    }

    let blocker_count = pending_blocker_count(connection, leaf).await?;
    if blocker_count > 0 {
        let attachment = match physical.detach_pending {
            None => LeafAttachment::Detached,
            Some(true) => LeafAttachment::DetachInterrupted,
            Some(false) => LeafAttachment::Attached,
        };
        return Ok(LeafInspection::PendingBlocked {
            leaf_name: leaf.leaf_name().to_owned(),
            blocker_count,
            expires_at,
            attachment,
        });
    }

    match physical.detach_pending {
        None => Ok(LeafInspection::Detached {
            leaf_name: leaf.leaf_name().to_owned(),
            expires_at,
        }),
        Some(true) => Ok(LeafInspection::DetachInterrupted {
            leaf_name: leaf.leaf_name().to_owned(),
            expires_at,
        }),
        Some(false) => {
            let now = database_now(connection).await?;
            if expires_at <= now {
                Ok(LeafInspection::Detachable {
                    leaf_name: leaf.leaf_name().to_owned(),
                    expires_at,
                })
            } else {
                Ok(LeafInspection::NotExpired {
                    leaf_name: leaf.leaf_name().to_owned(),
                    expires_at,
                })
            }
        }
    }
}

pub(crate) enum IndexRelationState {
    Absent,
    Attached,
    Foreign,
}

pub(crate) enum IndexRepairRemoval {
    Absent,
    Removed,
    Foreign,
}

pub(crate) async fn index_relation_state(
    connection: &mut PgConnection,
    leaf_name: &str,
    index_name: &str,
) -> Result<IndexRelationState, HistoryError> {
    let state: String = sqlx::query_scalar(
        "SELECT CASE
             WHEN to_regclass($2) IS NULL THEN 'absent'
             WHEN EXISTS (
                 SELECT 1 FROM pg_index
                 WHERE indexrelid = to_regclass($2)
                   AND indrelid = to_regclass($1)
             ) THEN 'attached'
             ELSE 'foreign'
         END",
    )
    .bind(leaf_name)
    .bind(index_name)
    .fetch_one(connection)
    .await?;
    match state.as_str() {
        "absent" => Ok(IndexRelationState::Absent),
        "attached" => Ok(IndexRelationState::Attached),
        "foreign" => Ok(IndexRelationState::Foreign),
        _ => Err(HistoryError::contract(
            "index relation probe returned an unknown state",
        )),
    }
}

async fn rollback_index_repair_claim(connection: &mut PgConnection) -> Result<(), HistoryError> {
    sqlx::query("ROLLBACK TO SAVEPOINT horsies_index_repair_claim")
        .execute(&mut *connection)
        .await?;
    sqlx::query("RELEASE SAVEPOINT horsies_index_repair_claim")
        .execute(connection)
        .await?;
    Ok(())
}

pub(crate) async fn remove_attached_index_for_repair(
    connection: &mut PgConnection,
    leaf_name: &str,
    index_name: &str,
) -> Result<IndexRepairRemoval, HistoryError> {
    if !is_safe_identifier(leaf_name) || !is_safe_identifier(index_name) {
        return Err(HistoryError::contract(
            "index repair names must be safe identifiers",
        ));
    }
    match index_relation_state(connection, leaf_name, index_name).await? {
        IndexRelationState::Absent => return Ok(IndexRepairRemoval::Absent),
        IndexRelationState::Foreign => return Ok(IndexRepairRemoval::Foreign),
        IndexRelationState::Attached => {}
    }

    let claim_name = format!("horsies_index_repair_{}", Uuid::new_v4().simple());
    sqlx::query("SAVEPOINT horsies_index_repair_claim")
        .execute(&mut *connection)
        .await?;
    let claim = async {
        sqlx::query(&format!(
            "ALTER INDEX IF EXISTS {index_name} RENAME TO {claim_name}"
        ))
        .execute(&mut *connection)
        .await?;
        match index_relation_state(connection, leaf_name, &claim_name).await? {
            IndexRelationState::Absent => Ok(IndexRepairRemoval::Absent),
            IndexRelationState::Foreign => Ok(IndexRepairRemoval::Foreign),
            IndexRelationState::Attached => {
                sqlx::query(&format!("DROP INDEX {claim_name}"))
                    .execute(&mut *connection)
                    .await?;
                Ok(IndexRepairRemoval::Removed)
            }
        }
    }
    .await;
    match claim {
        Ok(IndexRepairRemoval::Removed) => {
            sqlx::query("RELEASE SAVEPOINT horsies_index_repair_claim")
                .execute(connection)
                .await?;
            Ok(IndexRepairRemoval::Removed)
        }
        Ok(outcome @ (IndexRepairRemoval::Absent | IndexRepairRemoval::Foreign)) => {
            rollback_index_repair_claim(connection).await?;
            Ok(outcome)
        }
        Err(error) => {
            rollback_index_repair_claim(connection).await?;
            Err(error)
        }
    }
}

pub async fn create_daily_leaf<P: LoaderPublication>(
    connection: &mut PgConnection,
    command: &CreateDailyHistoryLeaf,
    publisher: &P,
) -> Result<LeafCreation, HistoryError> {
    let leaf = command.leaf();
    let Some(retention_class) = read_retention_class(connection, leaf.class_key()).await? else {
        return Ok(LeafCreation::RetentionClassAbsent {
            class_key: leaf.class_key().to_owned(),
        });
    };
    let is_forever = leaf.class_key() == FOREVER_CLASS_KEY
        && retention_class.duration.is_none()
        && retention_class.partition_interval.is_none();
    if !is_forever && retention_class.partition_interval != Some(DAILY) {
        return Ok(LeafCreation::ClassIntervalMismatch {
            class_key: leaf.class_key().to_owned(),
            partition_interval_days: interval_days(retention_class.partition_interval),
        });
    }
    let Some(parent_name) = history_class_parent(leaf.class_key(), &retention_class) else {
        return Ok(LeafCreation::ForeverClassLeaf {
            class_key: leaf.class_key().to_owned(),
        });
    };
    if daily_leaf_is_conformant(connection, leaf, &parent_name).await? {
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
    let catalog = read_leaf_catalog_row(connection, leaf.leaf_name()).await?;
    let id_index_name = catalog.as_ref().map_or_else(
        || leaf_id_index_name(leaf.leaf_name()),
        |row| row.id_index_name.clone(),
    );
    let physical = read_leaf_physical_state(
        connection,
        leaf.leaf_name(),
        &parent_name,
        &id_index_name,
        LeafPartitionBoundExpectation::Requested(leaf.bounds()),
        LeafIndexKind::History,
    )
    .await?;
    if physical.relation_exists != catalog.is_some() {
        let detail = if physical.relation_exists {
            "relation exists without a catalog row"
        } else {
            "catalog row exists without a relation"
        };
        return Ok(LeafCreation::CatalogConflict {
            leaf_name: leaf.leaf_name().to_owned(),
            kind: CatalogConflictKind::RelationWithoutCatalog,
            detail: detail.to_owned(),
        });
    }
    if let Some(catalog) = catalog {
        if catalog.parent_name != parent_name
            || catalog.class_key != leaf.class_key()
            || catalog.lower_anchor != leaf.bounds().lower()
            || catalog.upper_anchor != leaf.bounds().upper()
            || catalog.detached_at.is_some()
            || catalog.dropped_at.is_some()
        {
            return Ok(LeafCreation::CatalogConflict {
                leaf_name: leaf.leaf_name().to_owned(),
                kind: CatalogConflictKind::MetadataMismatch,
                detail: "existing leaf metadata differs from the request".to_owned(),
            });
        }
        if physical.partition_bound.as_deref() != Some(catalog.partition_bound.as_str())
            || !physical.partition_bound_matches_expected
        {
            return Ok(LeafCreation::CatalogConflict {
                leaf_name: leaf.leaf_name().to_owned(),
                kind: CatalogConflictKind::PhysicalNonconformant,
                detail: "attached leaf partition bound differs from catalog or requested bounds"
                    .to_owned(),
            });
        }
        if physical.detach_pending != Some(false) {
            return Ok(LeafCreation::CatalogConflict {
                leaf_name: leaf.leaf_name().to_owned(),
                kind: CatalogConflictKind::PhysicalNonconformant,
                detail: "required leaf is not attached to its cataloged parent".to_owned(),
            });
        }
        let ordering = read_leaf_ordering_index_exists(connection, leaf.leaf_name()).await?;
        if !physical.id_index_conformant || !ordering {
            if matches!(
                try_lock_relation_exclusive_for_transaction(connection, leaf.leaf_name()).await?,
                LeafLockAttempt::Busy
            ) {
                return Ok(LeafCreation::Busy {
                    leaf_name: leaf.leaf_name().to_owned(),
                });
            }
            if !physical.id_index_conformant {
                match remove_attached_index_for_repair(
                    connection,
                    leaf.leaf_name(),
                    &catalog.id_index_name,
                )
                .await?
                {
                    IndexRepairRemoval::Absent | IndexRepairRemoval::Removed => {}
                    IndexRepairRemoval::Foreign => {
                        return Ok(LeafCreation::CatalogConflict {
                            leaf_name: leaf.leaf_name().to_owned(),
                            kind: CatalogConflictKind::PhysicalNonconformant,
                            detail: "cataloged task-ID index name belongs to another relation"
                                .to_owned(),
                        });
                    }
                }
                sqlx::query(&format!(
                    "CREATE INDEX {} ON {} (task_id)",
                    catalog.id_index_name,
                    leaf.leaf_name()
                ))
                .execute(&mut *connection)
                .await?;
            }
            if !ordering {
                let ordering_name = leaf_enqueued_index_name(leaf.leaf_name());
                match remove_attached_index_for_repair(connection, leaf.leaf_name(), &ordering_name)
                    .await?
                {
                    IndexRepairRemoval::Absent | IndexRepairRemoval::Removed => {}
                    IndexRepairRemoval::Foreign => {
                        return Ok(LeafCreation::CatalogConflict {
                            leaf_name: leaf.leaf_name().to_owned(),
                            kind: CatalogConflictKind::PhysicalNonconformant,
                            detail: "derived ordering-index name belongs to another relation"
                                .to_owned(),
                        });
                    }
                }
                sqlx::query(&render_leaf_enqueued_index_ddl(leaf.leaf_name())?)
                    .execute(&mut *connection)
                    .await?;
            }
            sqlx::query(&format!("ANALYZE {}", leaf.leaf_name()))
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

    if matches!(
        try_lock_relation_exclusive_for_transaction(connection, &parent_name).await?,
        LeafLockAttempt::Busy
    ) {
        return Ok(LeafCreation::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        });
    }
    sqlx::query(&render_daily_leaf_ddl(&parent_name, leaf)?)
        .execute(&mut *connection)
        .await?;
    let recorded_bound = capture_partition_bound_utc(connection, leaf.leaf_name())
        .await?
        .ok_or_else(|| HistoryError::contract("new partition has no cataloged bound"))?;
    let sql = format!(
        "INSERT INTO {LEAF_CATALOG} (
             leaf_name, parent_name, class_key, lower_anchor, upper_anchor,
             index_schema_version, id_index_name, partition_bound,
             min_birth_at, min_birth_verified, created_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, TRUE,
                   statement_timestamp())"
    );
    sqlx::query(&sql)
        .bind(leaf.leaf_name())
        .bind(&parent_name)
        .bind(leaf.class_key())
        .bind(leaf.bounds().lower())
        .bind(leaf.bounds().upper())
        .bind(INDEX_SCHEMA_VERSION)
        .bind(&id_index_name)
        .bind(recorded_bound)
        .execute(&mut *connection)
        .await?;
    sqlx::query(&render_leaf_id_index_ddl(leaf.leaf_name())?)
        .execute(&mut *connection)
        .await?;
    sqlx::query(&render_leaf_enqueued_index_ddl(leaf.leaf_name())?)
        .execute(&mut *connection)
        .await?;
    sqlx::query(&format!("ANALYZE {}", leaf.leaf_name()))
        .execute(&mut *connection)
        .await?;
    publisher.republish(connection).await?;
    Ok(LeafCreation::Created {
        leaf_name: leaf.leaf_name().to_owned(),
        id_index_name,
    })
}

async fn daily_leaf_is_conformant(
    connection: &mut PgConnection,
    leaf: &LeafRef,
    parent_name: &str,
) -> Result<bool, HistoryError> {
    let Some(catalog) = read_leaf_catalog_row(connection, leaf.leaf_name()).await? else {
        return Ok(false);
    };
    if catalog.parent_name != parent_name
        || catalog.class_key != leaf.class_key()
        || catalog.lower_anchor != leaf.bounds().lower()
        || catalog.upper_anchor != leaf.bounds().upper()
        || catalog.detached_at.is_some()
        || catalog.dropped_at.is_some()
    {
        return Ok(false);
    }
    let physical = read_leaf_physical_state(
        connection,
        leaf.leaf_name(),
        parent_name,
        &catalog.id_index_name,
        LeafPartitionBoundExpectation::Requested(leaf.bounds()),
        LeafIndexKind::History,
    )
    .await?;
    if !physical.relation_exists
        || !physical.id_index_conformant
        || physical.detach_pending != Some(false)
        || physical.partition_bound.as_deref() != Some(catalog.partition_bound.as_str())
        || !physical.partition_bound_matches_expected
    {
        return Ok(false);
    }
    read_leaf_ordering_index_exists(connection, leaf.leaf_name()).await
}

pub async fn ensure_leaf_coverage<P: LoaderPublication>(
    connection: &mut PgConnection,
    command: &EnsureLeafCoverage,
    publisher: &P,
) -> Result<Vec<LeafCreation>, HistoryError> {
    let commands = match plan_daily_leaf_coverage(connection, command).await? {
        DailyCoveragePlan::Leaves(commands) => commands,
        DailyCoveragePlan::Refused(refusal) => return Ok(vec![refusal]),
    };
    let mut outcomes = Vec::with_capacity(commands.len());
    for create in commands {
        let outcome = create_daily_leaf(connection, &create, publisher).await?;
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

pub(crate) async fn plan_daily_leaf_coverage(
    connection: &mut PgConnection,
    command: &EnsureLeafCoverage,
) -> Result<DailyCoveragePlan, HistoryError> {
    let Some(retention_class) = read_retention_class(connection, command.class_key()).await? else {
        return Ok(DailyCoveragePlan::Refused(
            LeafCreation::RetentionClassAbsent {
                class_key: command.class_key().to_owned(),
            },
        ));
    };
    let is_forever = command.class_key() == FOREVER_CLASS_KEY
        && retention_class.duration.is_none()
        && retention_class.partition_interval.is_none();
    if !is_forever && retention_class.partition_interval != Some(DAILY) {
        return Ok(DailyCoveragePlan::Refused(
            LeafCreation::ClassIntervalMismatch {
                class_key: command.class_key().to_owned(),
                partition_interval_days: interval_days(retention_class.partition_interval),
            },
        ));
    }
    let Some(parent_name) = history_class_parent(command.class_key(), &retention_class) else {
        return Ok(DailyCoveragePlan::Refused(LeafCreation::ForeverClassLeaf {
            class_key: command.class_key().to_owned(),
        }));
    };
    let now = database_now(connection).await?;
    let today = now
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(|| HistoryError::contract("database timestamp cannot be truncated"))?;
    let mut commands = Vec::with_capacity(command.horizon_days() as usize + 1);
    for offset in 0..=command.horizon_days() {
        let lower = today + Duration::days(i64::from(offset));
        let bounds = LeafBounds::new(lower, lower + DAILY)
            .map_err(|error| HistoryError::contract(error.to_string()))?;
        let leaf_name = daily_leaf_name(&parent_name, lower)
            .map_err(|error| HistoryError::contract(error.to_string()))?;
        let leaf = LeafRef::new(leaf_name, command.class_key(), bounds)
            .map_err(|error| HistoryError::contract(error.to_string()))?;
        commands.push(
            CreateDailyHistoryLeaf::new(leaf)
                .map_err(|error| HistoryError::contract(error.to_string()))?,
        );
    }
    Ok(DailyCoveragePlan::Leaves(commands))
}

/// Detach one expired leaf without waiting for its advisory lock.
///
/// `pool` must preserve PostgreSQL session affinity. PgBouncer transaction
/// pooling does not preserve the session advisory lock used by this function.
pub async fn detach_expired_leaf<P, Q>(
    pool: &PgPool,
    command: &DetachExpiredHistoryLeaf,
    publisher: &P,
    quarantine: &Q,
) -> Result<DetachExpiredLeafOutcome, HistoryError>
where
    P: LoaderPublication,
    Q: LeafBlockerQuarantine,
{
    let leaf = command.leaf();
    let mut connection = SessionConnection::new(pool.acquire().await?);
    let prior_timeouts =
        read_prior_timeouts(&mut connection, command.statement_timeout_ms()).await?;
    if matches!(
        try_lock_leaf_for_session(&mut connection, leaf.class_key(), leaf.bounds().lower()).await?,
        LeafLockAttempt::Busy
    ) {
        connection.mark_reusable();
        return Ok(DetachExpiredLeafOutcome::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        });
    }
    let result = detach_locked(&mut connection, command, publisher, quarantine).await;
    let cleanup = restore_timeouts_and_unlock(&mut connection, leaf, &prior_timeouts).await;
    if cleanup.is_ok() {
        connection.mark_reusable();
    }
    match (result, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(outcome), Ok(())) => Ok(outcome),
    }
}

async fn detach_locked<P, Q>(
    connection: &mut PgConnection,
    command: &DetachExpiredHistoryLeaf,
    publisher: &P,
    quarantine: &Q,
) -> Result<DetachExpiredLeafOutcome, HistoryError>
where
    P: LoaderPublication,
    Q: LeafBlockerQuarantine,
{
    let leaf = command.leaf();
    let mut inspection = inspect_leaf(connection, &InspectHistoryLeaf::new(leaf.clone())).await?;
    match &inspection {
        LeafInspection::Detachable { .. } => {}
        LeafInspection::PendingBlocked {
            attachment: LeafAttachment::Attached,
            expires_at,
            ..
        } if command.quarantine_horizon().is_some() => {
            if *expires_at > database_now(connection).await? {
                return Ok(DetachExpiredLeafOutcome::Inspection(inspection));
            }
            let horizon = command.quarantine_horizon().expect("matched Some horizon");
            if let QuarantineResult::Refused(refusal) =
                quarantine.quarantine(connection, leaf, horizon).await?
            {
                return Ok(DetachExpiredLeafOutcome::QuarantineRefused(refusal));
            }
            inspection = inspect_leaf(connection, &InspectHistoryLeaf::new(leaf.clone())).await?;
            if !matches!(inspection, LeafInspection::Detachable { .. }) {
                return Ok(DetachExpiredLeafOutcome::Inspection(inspection));
            }
        }
        _ => return Ok(DetachExpiredLeafOutcome::Inspection(inspection)),
    }
    set_ddl_timeouts(connection, command.statement_timeout_ms()).await?;
    let retention = read_retention_class(connection, leaf.class_key())
        .await?
        .ok_or_else(|| HistoryError::contract("detachable class disappeared"))?;
    let parent = retention
        .finite_parent_name
        .ok_or_else(|| HistoryError::contract("detachable class has no finite parent"))?;
    let detach = sqlx::query(&format!(
        "ALTER TABLE {parent} DETACH PARTITION {} CONCURRENTLY",
        leaf.leaf_name()
    ))
    .execute(&mut *connection)
    .await;
    match detach {
        Ok(_) => {}
        Err(error) if is_lock_not_available(&error) => {
            return Ok(DetachExpiredLeafOutcome::Busy {
                leaf_name: leaf.leaf_name().to_owned(),
            });
        }
        Err(error) => return Err(error.into()),
    }
    record_detached(connection, leaf.leaf_name()).await?;
    publisher.republish(connection).await?;
    Ok(DetachExpiredLeafOutcome::Inspection(
        inspect_leaf(connection, &InspectHistoryLeaf::new(leaf.clone())).await?,
    ))
}

/// Finalize an interrupted detach without waiting for its advisory lock.
///
/// `pool` must preserve PostgreSQL session affinity. PgBouncer transaction
/// pooling does not preserve the session advisory lock used by this function.
pub async fn finalize_interrupted_detach<P: LoaderPublication>(
    pool: &PgPool,
    command: &FinalizeInterruptedLeafDetach,
    publisher: &P,
) -> Result<FinalizeInterruptedLeafOutcome, HistoryError> {
    let leaf = command.leaf();
    let mut connection = SessionConnection::new(pool.acquire().await?);
    let prior_timeouts =
        read_prior_timeouts(&mut connection, command.statement_timeout_ms()).await?;
    if matches!(
        try_lock_leaf_for_session(&mut connection, leaf.class_key(), leaf.bounds().lower()).await?,
        LeafLockAttempt::Busy
    ) {
        connection.mark_reusable();
        return Ok(FinalizeInterruptedLeafOutcome::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        });
    }
    let result = finalize_locked(&mut connection, command, publisher).await;
    let cleanup = restore_timeouts_and_unlock(&mut connection, leaf, &prior_timeouts).await;
    if cleanup.is_ok() {
        connection.mark_reusable();
    }
    match (result, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(outcome), Ok(())) => Ok(outcome),
    }
}

async fn finalize_locked<P: LoaderPublication>(
    connection: &mut PgConnection,
    command: &FinalizeInterruptedLeafDetach,
    publisher: &P,
) -> Result<FinalizeInterruptedLeafOutcome, HistoryError> {
    let leaf = command.leaf();
    set_ddl_timeouts(connection, command.statement_timeout_ms()).await?;
    let inspection = inspect_leaf(connection, &InspectHistoryLeaf::new(leaf.clone())).await?;
    match inspection {
        LeafInspection::Detached { .. } => {
            record_detached(connection, leaf.leaf_name()).await?;
            publisher.republish(connection).await?;
        }
        LeafInspection::DetachInterrupted { .. } => {
            let retention = read_retention_class(connection, leaf.class_key())
                .await?
                .ok_or_else(|| HistoryError::contract("interrupted class disappeared"))?;
            let parent = retention
                .finite_parent_name
                .ok_or_else(|| HistoryError::contract("interrupted class has no finite parent"))?;
            let finalize = sqlx::query(&format!(
                "ALTER TABLE {parent} DETACH PARTITION {} FINALIZE",
                leaf.leaf_name()
            ))
            .execute(&mut *connection)
            .await;
            match finalize {
                Ok(_) => {}
                Err(error) if is_lock_not_available(&error) => {
                    return Ok(FinalizeInterruptedLeafOutcome::Busy {
                        leaf_name: leaf.leaf_name().to_owned(),
                    });
                }
                Err(error) => return Err(error.into()),
            }
            record_detached(connection, leaf.leaf_name()).await?;
            publisher.republish(connection).await?;
        }
        other => return Ok(FinalizeInterruptedLeafOutcome::Inspection(other)),
    }
    Ok(FinalizeInterruptedLeafOutcome::Inspection(
        inspect_leaf(connection, &InspectHistoryLeaf::new(leaf.clone())).await?,
    ))
}

pub async fn drop_detached_leaf<P: LoaderPublication>(
    connection: &mut PgConnection,
    command: &DropDetachedHistoryLeaf,
    publisher: &P,
) -> Result<LeafDrop, HistoryError> {
    let leaf = command.leaf();
    let mut inspection = inspect_leaf(connection, &InspectHistoryLeaf::new(leaf.clone())).await?;
    if !matches!(inspection, LeafInspection::Detached { .. }) {
        return Ok(LeafDrop::Inspection(inspection));
    }
    if matches!(
        try_lock_leaf_for_transaction(connection, leaf.class_key(), leaf.bounds().lower()).await?,
        LeafLockAttempt::Busy
    ) {
        return Ok(LeafDrop::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        });
    }
    inspection = inspect_leaf(connection, &InspectHistoryLeaf::new(leaf.clone())).await?;
    if !matches!(inspection, LeafInspection::Detached { .. }) {
        return Ok(LeafDrop::Inspection(inspection));
    }
    if publisher
        .references_leaf(connection, leaf.leaf_name())
        .await?
    {
        return Ok(LeafDrop::RefusedLoaderReferences {
            leaf_name: leaf.leaf_name().to_owned(),
        });
    }
    if matches!(
        try_lock_relation_exclusive_for_transaction(connection, leaf.leaf_name()).await?,
        LeafLockAttempt::Busy
    ) {
        return Ok(LeafDrop::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        });
    }
    sqlx::query(&format!("DROP TABLE {}", leaf.leaf_name()))
        .execute(&mut *connection)
        .await?;
    let sql = format!(
        "UPDATE {LEAF_CATALOG}
         SET detached_at = COALESCE(detached_at, statement_timestamp()),
             dropped_at = statement_timestamp()
         WHERE leaf_name = $1"
    );
    sqlx::query(&sql)
        .bind(leaf.leaf_name())
        .execute(connection)
        .await?;
    Ok(LeafDrop::Dropped {
        leaf_name: leaf.leaf_name().to_owned(),
    })
}

async fn pending_blocker_count(
    connection: &mut PgConnection,
    leaf: &LeafRef,
) -> Result<i64, HistoryError> {
    let sql = format!(
        "SELECT count(*) FROM {WORKFLOW_PHASE2_PENDING}
         WHERE recovery_source = 'HISTORY' AND history_class = $1
           AND history_anchor >= $2 AND history_anchor < $3"
    );
    Ok(sqlx::query_scalar(&sql)
        .bind(leaf.class_key())
        .bind(leaf.bounds().lower())
        .bind(leaf.bounds().upper())
        .fetch_one(connection)
        .await?)
}

async fn record_detached(
    connection: &mut PgConnection,
    leaf_name: &str,
) -> Result<(), HistoryError> {
    let sql = format!(
        "UPDATE {LEAF_CATALOG}
         SET detached_at = COALESCE(detached_at, statement_timestamp())
         WHERE leaf_name = $1"
    );
    sqlx::query(&sql)
        .bind(leaf_name)
        .execute(connection)
        .await?;
    Ok(())
}

async fn set_ddl_timeouts(
    connection: &mut PgConnection,
    timeout_ms: Option<u64>,
) -> Result<(), HistoryError> {
    if let Some(timeout_ms) = timeout_ms {
        sqlx::query("SELECT set_config('statement_timeout', $1, false)")
            .bind(format!("{timeout_ms}ms"))
            .execute(&mut *connection)
            .await?;
    }
    sqlx::query("SELECT set_config('lock_timeout', $1, false)")
        .bind(format!("{LEAF_DDL_LOCK_TIMEOUT_MS}ms"))
        .execute(connection)
        .await?;
    Ok(())
}

struct PriorTimeouts {
    statement: Option<String>,
    lock: String,
}

async fn read_prior_timeouts(
    connection: &mut PgConnection,
    timeout_ms: Option<u64>,
) -> Result<PriorTimeouts, HistoryError> {
    let statement = if timeout_ms.is_some() {
        Some(
            sqlx::query_scalar("SHOW statement_timeout")
                .fetch_one(&mut *connection)
                .await?,
        )
    } else {
        None
    };
    let lock = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(connection)
        .await?;
    Ok(PriorTimeouts { statement, lock })
}

async fn restore_timeouts_and_unlock(
    connection: &mut PgConnection,
    leaf: &LeafRef,
    prior: &PriorTimeouts,
) -> Result<(), HistoryError> {
    let restore_statement = if let Some(prior_timeout) = prior.statement.as_deref() {
        sqlx::query("SELECT set_config('statement_timeout', $1, false)")
            .bind(prior_timeout)
            .execute(&mut *connection)
            .await
            .map(|_| ())
            .map_err(HistoryError::from)
    } else {
        Ok(())
    };
    let restore_lock = sqlx::query("SELECT set_config('lock_timeout', $1, false)")
        .bind(&prior.lock)
        .execute(&mut *connection)
        .await
        .map(|_| ())
        .map_err(HistoryError::from);
    let unlock = unlock_leaf_for_session(connection, leaf.class_key(), leaf.bounds().lower()).await;
    restore_statement.and(restore_lock).and(unlock)
}

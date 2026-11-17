//! Typed lifecycle for one task-history leaf.

use std::ops::{Deref, DerefMut};

use chrono::{DateTime, Duration, Timelike, Utc};
use sqlx::pool::PoolConnection;
use sqlx::{Connection, PgConnection, PgPool, Postgres};
use uuid::Uuid;

use crate::core::history::commands::{
    is_safe_identifier, open_end_anchor, CreateDailyHistoryLeaf, DetachExpiredHistoryLeaf,
    DropDetachedHistoryLeaf, EnsureLeafCoverage, FinalizeInterruptedLeafDetach, InspectHistoryLeaf,
    LeafBounds, LeafRef,
};
use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;
use crate::core::history::ddl::runtime_names::{
    daily_leaf_name, leaf_enqueued_index_name, leaf_id_index_name, render_daily_leaf_ddl,
    render_leaf_enqueued_index_ddl, render_leaf_id_index_ddl,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{
    HEARTBEAT_CLASS_KEY, LEAF_CATALOG, TASK_HISTORY_FOREVER, TASK_HISTORY_FOREVER_LEAF,
    WORKFLOW_PHASE2_PENDING,
};
use crate::core::history::outcomes::{
    CatalogConflictKind, LeafAttachment, LeafCreation, LeafDrop, LeafInspection,
};

use super::catalog::{
    database_now, leaf_identity_index_name, read_leaf_catalog_row, read_leaf_ordering_index_exists,
    read_leaf_physical_state, read_open_ended_forever_leaf, read_retention_class, LeafIndexKind,
    LeafLowerBound, LeafPartitionBoundExpectation, RetentionClassKind, RetentionClassRow,
    AT_LOWER_BOUND_KIND, INDEX_SCHEMA_VERSION,
};
use super::locks::{
    is_lock_not_available, try_lock_leaf_for_transaction,
    try_lock_relation_exclusive_for_transaction, unlock_leaf_for_session, LeafLockAttempt,
};
use super::publication::LoaderPublication;
use super::retirement::{
    begin_leaf_detach, drop_detached_retention_leaf, record_leaf_detached, recorded_detach,
    BeginDetach, DetachRecord, RecordedDetach,
};

const DAILY: Duration = Duration::days(1);
const LEAF_DDL_LOCK_TIMEOUT_MS: u64 = 2_000;

/// One leaf that coverage plans to ensure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PlannedLeaf {
    Daily(CreateDailyHistoryLeaf),
}

impl PlannedLeaf {
    pub(crate) fn leaf(&self) -> &LeafRef {
        match self {
            Self::Daily(command) => command.leaf(),
        }
    }
}

/// Creates or repairs one planned leaf.
pub(crate) async fn create_planned_leaf<P: LoaderPublication>(
    connection: &mut PgConnection,
    planned: &PlannedLeaf,
    publisher: &P,
) -> Result<LeafCreation, HistoryError> {
    match planned {
        PlannedLeaf::Daily(create) => create_daily_leaf(connection, create, publisher).await,
    }
}

pub(crate) enum CoveragePlan {
    Leaves(Vec<PlannedLeaf>),
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
pub enum DetachExpiredLeafOutcome {
    Busy {
        leaf_name: String,
    },
    /// This call detached the leaf, recorded the detach in the catalog and
    /// republished the readers.
    Detached {
        leaf_name: String,
    },
    /// This call did not detach the leaf; the inspection names the state that
    /// kept it. `LeafInspection::Detached` means an earlier call detached it.
    Inspection(LeafInspection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalizeInterruptedLeafOutcome {
    Busy { leaf_name: String },
    Inspection(LeafInspection),
}

/// Where the daily or open-ended history leaves of a class live, or the
/// refusal when the class holds no history leaves of that shape.
enum HistoryClassParent {
    /// The `forever` class: one open-ended leaf under `horsies_task_history_forever`.
    Forever(String),
    /// A period class: daily leaves under `horsies_task_history_<class_key>`.
    Period(String),
    /// The heartbeat class: hourly leaves, not history leaves.
    NotDaily(LeafCreation),
}

fn history_class_parent(
    retention_class: &RetentionClassRow,
) -> Result<HistoryClassParent, HistoryError> {
    match retention_class.kind {
        RetentionClassKind::Forever => {
            Ok(HistoryClassParent::Forever(retention_class.parent_name()?))
        }
        RetentionClassKind::Period(_) => {
            Ok(HistoryClassParent::Period(retention_class.parent_name()?))
        }
        // An hourly class: its interval is less than one whole day.
        RetentionClassKind::Heartbeat(_) => Ok(HistoryClassParent::NotDaily(
            LeafCreation::ClassIntervalMismatch {
                class_key: retention_class.class_key.clone(),
                partition_interval_days: Some(0),
            },
        )),
    }
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
    let (parent_name, eligible_at) = match retention_class.retention() {
        None => {
            return Ok(LeafInspection::ForeverClassLeaf {
                class_key: leaf.class_key().to_owned(),
            });
        }
        Some(retention) => (
            retention_class.parent_name()?,
            leaf.bounds().upper() + retention,
        ),
    };
    let parent_name = parent_name.as_str();
    let catalog = read_leaf_catalog_row(connection, leaf.leaf_name()).await?;
    let id_index_name = leaf_identity_index_name(leaf.class_key(), leaf.leaf_name())?;
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
            eligible_at: Some(eligible_at),
        });
    };

    if !catalog.describes(leaf.class_key(), leaf.bounds()) {
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
                eligible_at: Some(eligible_at),
            })
        };
    }
    if !physical.parent_exists {
        return Err(HistoryError::HistoryParentAbsent(format!(
            "finite history parent {parent_name:?} does not exist"
        )));
    }
    if physical.detach_pending.is_some()
        && (!physical.partition_bound_matches_expected || !physical.id_index_conformant)
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
            eligible_at,
            attachment,
        });
    }

    match physical.detach_pending {
        None => Ok(LeafInspection::Detached {
            leaf_name: leaf.leaf_name().to_owned(),
            eligible_at,
        }),
        Some(true) => Ok(LeafInspection::DetachInterrupted {
            leaf_name: leaf.leaf_name().to_owned(),
            eligible_at,
        }),
        Some(false) => {
            let now = database_now(connection).await?;
            if eligible_at <= now {
                Ok(LeafInspection::Detachable {
                    leaf_name: leaf.leaf_name().to_owned(),
                    eligible_at,
                })
            } else {
                Ok(LeafInspection::NotExpired {
                    leaf_name: leaf.leaf_name().to_owned(),
                    eligible_at,
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
    /// The index relation lock was not granted within
    /// `INDEX_REPAIR_LOCK_TIMEOUT_MS`.
    Busy,
}

/// Lock wait of the index rename and drop of a repair; a longer wait is
/// `IndexRepairRemoval::Busy`.
const INDEX_REPAIR_LOCK_TIMEOUT_MS: u64 = 2_000;

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
    let prior_lock_timeout: String = sqlx::query_scalar("SELECT current_setting('lock_timeout')")
        .fetch_one(&mut *connection)
        .await?;
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(format!("{INDEX_REPAIR_LOCK_TIMEOUT_MS}ms"))
        .execute(&mut *connection)
        .await?;
    let removal = claim_and_drop_index(connection, leaf_name, index_name).await;
    let restored = sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(&prior_lock_timeout)
        .execute(&mut *connection)
        .await;
    let removal = removal?;
    restored?;
    Ok(removal)
}

/// Rename the index to a private name, which takes its relation lock, check
/// its owner again under that lock, and drop it. A savepoint restores the name
/// when the owner changed or the lock was not granted.
async fn claim_and_drop_index(
    connection: &mut PgConnection,
    leaf_name: &str,
    index_name: &str,
) -> Result<IndexRepairRemoval, HistoryError> {
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
        Ok(
            outcome @ (IndexRepairRemoval::Absent
            | IndexRepairRemoval::Foreign
            | IndexRepairRemoval::Busy),
        ) => {
            rollback_index_repair_claim(connection).await?;
            Ok(outcome)
        }
        Err(HistoryError::Database(error)) if is_lock_not_available(&error) => {
            rollback_index_repair_claim(connection).await?;
            Ok(IndexRepairRemoval::Busy)
        }
        Err(error) => {
            rollback_index_repair_claim(connection).await?;
            Err(error)
        }
    }
}

/// Creates or repairs one daily leaf. A daily `forever` leaf that the attached
/// open-ended `forever` leaf covers is refused with `CoveredByOpenEndedLeaf`,
/// and nothing is created.
pub async fn create_daily_leaf<P: LoaderPublication>(
    connection: &mut PgConnection,
    command: &CreateDailyHistoryLeaf,
    publisher: &P,
) -> Result<LeafCreation, HistoryError> {
    let leaf = command.leaf();
    let covering = match leaf.class_key() == FOREVER_CLASS_KEY {
        true => read_open_ended_forever_leaf(connection)
            .await?
            .filter(|open| open.lower_anchor < LeafLowerBound::At(leaf.bounds().upper())),
        false => None,
    };
    match covering {
        Some(open) => Ok(LeafCreation::CoveredByOpenEndedLeaf {
            leaf_name: leaf.leaf_name().to_owned(),
            open_leaf_name: open.leaf_name,
        }),
        None => create_leaf(connection, leaf, publisher).await,
    }
}

async fn create_leaf<P: LoaderPublication>(
    connection: &mut PgConnection,
    leaf: &LeafRef,
    publisher: &P,
) -> Result<LeafCreation, HistoryError> {
    let Some(retention_class) = read_retention_class(connection, leaf.class_key()).await? else {
        return Ok(LeafCreation::RetentionClassAbsent {
            class_key: leaf.class_key().to_owned(),
        });
    };
    let parent_name = match history_class_parent(&retention_class)? {
        HistoryClassParent::Forever(parent) | HistoryClassParent::Period(parent) => parent,
        HistoryClassParent::NotDaily(refusal) => return Ok(refusal),
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
    let id_index_name = leaf_id_index_name(leaf.leaf_name());
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
            // Read the state again under the relation lock: another session
            // can have repaired, detached or replaced the leaf meanwhile.
            let physical = read_leaf_physical_state(
                connection,
                leaf.leaf_name(),
                &parent_name,
                &id_index_name,
                LeafPartitionBoundExpectation::Requested(leaf.bounds()),
                LeafIndexKind::History,
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
            let ordering = read_leaf_ordering_index_exists(connection, leaf.leaf_name()).await?;
            if physical.id_index_conformant && ordering {
                return Ok(LeafCreation::AlreadyConformant {
                    leaf_name: leaf.leaf_name().to_owned(),
                });
            }
            if !physical.id_index_conformant {
                match remove_attached_index_for_repair(connection, leaf.leaf_name(), &id_index_name)
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
                            detail: "cataloged task-ID index name belongs to another relation"
                                .to_owned(),
                        });
                    }
                }
                sqlx::query(&format!(
                    "CREATE INDEX {id_index_name} ON {} (task_id)",
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
                    IndexRepairRemoval::Busy => {
                        return Ok(LeafCreation::Busy {
                            leaf_name: leaf.leaf_name().to_owned(),
                        });
                    }
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
                id_index_name,
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
    if !catalog.describes(leaf.class_key(), leaf.bounds())
        || catalog.detached_at.is_some()
        || catalog.dropped_at.is_some()
    {
        return Ok(false);
    }
    let physical = read_leaf_physical_state(
        connection,
        leaf.leaf_name(),
        parent_name,
        &leaf_id_index_name(leaf.leaf_name()),
        LeafPartitionBoundExpectation::Requested(leaf.bounds()),
        LeafIndexKind::History,
    )
    .await?;
    if !physical.relation_exists
        || !physical.id_index_conformant
        || physical.detach_pending != Some(false)
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
    let commands = match plan_leaf_coverage(connection, command).await? {
        CoveragePlan::Leaves(commands) => commands,
        CoveragePlan::Refused(refusal) => return Ok(vec![refusal]),
    };
    let mut outcomes = Vec::with_capacity(commands.len());
    for planned in commands {
        let outcome = create_planned_leaf(connection, &planned, publisher).await?;
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

/// Plans the leaves a class needs: one daily leaf per horizon day for a finite
/// class; the one open-ended leaf for `forever`.
pub(crate) async fn plan_leaf_coverage(
    connection: &mut PgConnection,
    command: &EnsureLeafCoverage,
) -> Result<CoveragePlan, HistoryError> {
    let Some(retention_class) = read_retention_class(connection, command.class_key()).await? else {
        return Ok(CoveragePlan::Refused(LeafCreation::RetentionClassAbsent {
            class_key: command.class_key().to_owned(),
        }));
    };
    let parent_name = match history_class_parent(&retention_class)? {
        HistoryClassParent::Forever(_) => return plan_static_forever_leaf(connection).await,
        HistoryClassParent::Period(parent) => parent,
        HistoryClassParent::NotDaily(refusal) => return Ok(CoveragePlan::Refused(refusal)),
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
        commands.push(PlannedLeaf::Daily(
            CreateDailyHistoryLeaf::new(leaf)
                .map_err(|error| HistoryError::contract(error.to_string()))?,
        ));
    }
    Ok(CoveragePlan::Leaves(commands))
}

/// The partition bound of the static `forever` leaf, as `pg_get_expr` renders it
/// in UTC.
pub(crate) const STATIC_FOREVER_LEAF_BOUND: &str =
    "FOR VALUES FROM (MINVALUE) TO ('9999-01-01 00:00:00+00')";

/// The static `forever` leaf is install-owned: coverage verifies it and never
/// creates or repairs it.
async fn plan_static_forever_leaf(
    connection: &mut PgConnection,
) -> Result<CoveragePlan, HistoryError> {
    match verify_static_forever_leaf(connection).await? {
        LeafCreation::AlreadyConformant { .. } => Ok(CoveragePlan::Leaves(Vec::new())),
        refusal => Ok(CoveragePlan::Refused(refusal)),
    }
}

/// Verify the static `forever` leaf: its catalog row is the baseline row, and
/// its relation, bound and two indexes match it. Reads only.
///
/// Returns `AlreadyConformant` or a `CatalogConflict`: any difference is a
/// conflict for an operator. Fails with `HistoryParentAbsent` when the forever
/// parent is missing.
pub async fn verify_static_forever_leaf(
    connection: &mut PgConnection,
) -> Result<LeafCreation, HistoryError> {
    let conflict = |kind: CatalogConflictKind, detail: &str| LeafCreation::CatalogConflict {
        leaf_name: TASK_HISTORY_FOREVER_LEAF.to_owned(),
        kind,
        detail: detail.to_owned(),
    };
    match read_leaf_catalog_row(connection, TASK_HISTORY_FOREVER_LEAF).await? {
        None => {
            return Ok(conflict(
                CatalogConflictKind::RelationWithoutCatalog,
                "the static forever leaf has no catalog row",
            ));
        }
        Some(row)
            if row.class_key == FOREVER_CLASS_KEY
                && row.lower_anchor == LeafLowerBound::Open
                && row.upper_anchor == open_end_anchor()
                && row.index_schema_version == INDEX_SCHEMA_VERSION
                && row.detached_at.is_none()
                && row.dropped_at.is_none() => {}
        Some(_) => {
            return Ok(conflict(
                CatalogConflictKind::MetadataMismatch,
                "the static forever leaf catalog row is not the baseline row",
            ));
        }
    }
    let physical = read_leaf_physical_state(
        connection,
        TASK_HISTORY_FOREVER_LEAF,
        TASK_HISTORY_FOREVER,
        &leaf_id_index_name(TASK_HISTORY_FOREVER_LEAF),
        LeafPartitionBoundExpectation::CatalogOnly,
        LeafIndexKind::History,
    )
    .await?;
    if !physical.parent_exists {
        return Err(HistoryError::HistoryParentAbsent(format!(
            "forever parent {TASK_HISTORY_FOREVER:?} does not exist"
        )));
    }
    let conformant = physical.relation_exists
        && physical.detach_pending == Some(false)
        && physical.partition_bound.as_deref() == Some(STATIC_FOREVER_LEAF_BOUND)
        && physical.id_index_conformant
        && read_leaf_ordering_index_exists(connection, TASK_HISTORY_FOREVER_LEAF).await?;
    match conformant {
        true => Ok(LeafCreation::AlreadyConformant {
            leaf_name: TASK_HISTORY_FOREVER_LEAF.to_owned(),
        }),
        false => Ok(conflict(
            CatalogConflictKind::PhysicalNonconformant,
            "the static forever leaf relation, bound or indexes differ from the baseline",
        )),
    }
}

/// Detach one expired PERIOD or HEARTBEAT leaf without waiting for its
/// advisory lock.
///
/// `horsies_begin_leaf_detach` classifies the leaf and takes its session
/// lock; `DETACH PARTITION ... CONCURRENTLY` runs under the command's
/// statement timeout and a 2 s lock timeout; `horsies_record_leaf_detached`
/// records it and, when the published manifest names the leaf, the readers
/// are published in the same transaction. Any state other than `DETACHABLE`
/// is returned as its inspection and nothing is detached.
/// `LeafInspection::Detached` means the leaf has no parent (an earlier call,
/// or a detach out of band): `horsies_begin_leaf_detach` then records the
/// detach and, when the published manifest names the leaf, publishes the
/// readers in its own statement, so the drop that follows is not refused for
/// the manifest.
///
/// `pool` must preserve PostgreSQL session affinity. PgBouncer transaction
/// pooling does not preserve the session advisory lock used by this function.
pub async fn detach_expired_leaf<P: LoaderPublication>(
    pool: &PgPool,
    command: &DetachExpiredHistoryLeaf,
    publisher: &P,
) -> Result<DetachExpiredLeafOutcome, HistoryError> {
    let leaf = command.leaf();
    let mut connection = SessionConnection::new(pool.acquire().await?);
    let detach = detach_retention_leaf(
        &mut connection,
        leaf,
        DetachStep::Concurrently,
        command.statement_timeout_ms(),
        publisher,
    )
    .await?;
    Ok(match detach {
        RetentionDetach::Busy => DetachExpiredLeafOutcome::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        },
        RetentionDetach::Recorded { .. } => DetachExpiredLeafOutcome::Detached {
            leaf_name: leaf.leaf_name().to_owned(),
        },
        RetentionDetach::Inspection(inspection) => DetachExpiredLeafOutcome::Inspection(inspection),
    })
}

/// Finalize a concurrent detach that a crash or a timeout left pending,
/// without waiting for its advisory lock.
///
/// Acts on `DETACH_INTERRUPTED` only: `DETACH PARTITION ... FINALIZE`, then
/// the record (and the publication) as in [`detach_expired_leaf`]. A leaf
/// found already detached is returned as `LeafInspection::Detached`:
/// `horsies_begin_leaf_detach` records its detach, and this function sends no
/// record of its own. The result is `LeafInspection::Detached` once the
/// detach is recorded; every other state is returned unchanged.
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
    let finalize = detach_retention_leaf(
        &mut connection,
        leaf,
        DetachStep::Finalize,
        command.statement_timeout_ms(),
        publisher,
    )
    .await?;
    Ok(match finalize {
        RetentionDetach::Busy => FinalizeInterruptedLeafOutcome::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        },
        RetentionDetach::Recorded { eligible_at } => {
            FinalizeInterruptedLeafOutcome::Inspection(LeafInspection::Detached {
                leaf_name: leaf.leaf_name().to_owned(),
                eligible_at,
            })
        }
        RetentionDetach::Inspection(inspection) => {
            FinalizeInterruptedLeafOutcome::Inspection(inspection)
        }
    })
}

/// The detach statement one call runs, and the state it acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetachStep {
    /// `DETACH PARTITION ... CONCURRENTLY`; acts on `DETACHABLE`.
    Concurrently,
    /// `DETACH PARTITION ... FINALIZE`; acts on `DETACH_INTERRUPTED`.
    Finalize,
}

impl DetachStep {
    fn keyword(self) -> &'static str {
        match self {
            Self::Concurrently => "CONCURRENTLY",
            Self::Finalize => "FINALIZE",
        }
    }
}

/// What one detach or finalize call ended with.
enum RetentionDetach {
    /// Another session holds the leaf lock, or the detach statement met a
    /// held relation lock.
    Busy,
    /// The detach is recorded in the catalog (by this call or before it).
    Recorded { eligible_at: DateTime<Utc> },
    /// The call did not detach the leaf; the inspection names its state.
    Inspection(LeafInspection),
}

/// One detach or finalize of a PERIOD or HEARTBEAT leaf on one session
/// connection.
///
/// The connection is marked reusable only when it holds no leaf lock and its
/// session timeouts are the ones it had before the call. A cancel, or a
/// failure after which that is not known, leaves it unmarked: it is closed,
/// and PostgreSQL releases the session lock.
async fn detach_retention_leaf<P: LoaderPublication>(
    connection: &mut SessionConnection,
    leaf: &LeafRef,
    step: DetachStep,
    statement_timeout_ms: Option<u64>,
    publisher: &P,
) -> Result<RetentionDetach, HistoryError> {
    let (parent_name, inspection) = match begin_leaf_detach(connection, leaf).await? {
        BeginDetach::Locked {
            parent_name,
            inspection,
        } => (parent_name, inspection),
        BeginDetach::Busy => {
            connection.mark_reusable();
            return Ok(RetentionDetach::Busy);
        }
        // `horsies_begin_leaf_detach` recorded the detach of a leaf it found
        // already detached; that state is returned as it is.
        BeginDetach::Unlocked(inspection) => {
            connection.mark_reusable();
            return Ok(RetentionDetach::Inspection(inspection));
        }
    };
    // The session holds the leaf lock from here on.
    let eligible_at = match (&inspection, step) {
        (LeafInspection::Detachable { eligible_at, .. }, DetachStep::Concurrently)
        | (LeafInspection::DetachInterrupted { eligible_at, .. }, DetachStep::Finalize) => {
            *eligible_at
        }
        _ => {
            let released = restore_timeouts_and_unlock(connection, leaf, None).await;
            return release_outcome(
                connection,
                Ok(RetentionDetach::Inspection(inspection)),
                released,
            );
        }
    };
    let prior = match read_prior_timeouts(connection, statement_timeout_ms).await {
        Ok(prior) => prior,
        Err(error) => {
            let released = restore_timeouts_and_unlock(connection, leaf, None).await;
            return release_outcome(connection, Err(error), released);
        }
    };
    let outcome = match run_detach_statement(
        connection,
        leaf,
        &parent_name,
        step,
        statement_timeout_ms,
    )
    .await
    {
        Ok(PhysicalDetach::Busy) => Ok(RetentionDetach::Busy),
        Ok(PhysicalDetach::Detached) => {
            record_detach(connection, leaf, eligible_at, publisher).await
        }
        Err(error) => Err(error),
    };
    let released = restore_timeouts_and_unlock(connection, leaf, Some(&prior)).await;
    release_outcome(connection, outcome, released)
}

/// The outcome of a call that held the leaf lock, after the release. The
/// connection is marked reusable only when the release succeeded; the first
/// error wins.
fn release_outcome(
    connection: &mut SessionConnection,
    outcome: Result<RetentionDetach, HistoryError>,
    released: Result<(), HistoryError>,
) -> Result<RetentionDetach, HistoryError> {
    match released {
        Ok(()) => {
            connection.mark_reusable();
            outcome
        }
        Err(release_error) => match outcome {
            Err(error) => Err(error),
            Ok(_) => Err(release_error),
        },
    }
}

enum PhysicalDetach {
    Detached,
    Busy,
}

/// `ALTER TABLE <parent> DETACH PARTITION <leaf> CONCURRENTLY | FINALIZE`
/// under the statement timeout and the DDL lock timeout. The caller holds the
/// leaf's session lock and restores the timeouts. A held relation lock
/// (`55P03`) is `Busy`.
async fn run_detach_statement(
    connection: &mut PgConnection,
    leaf: &LeafRef,
    parent_name: &str,
    step: DetachStep,
    statement_timeout_ms: Option<u64>,
) -> Result<PhysicalDetach, HistoryError> {
    if !is_safe_identifier(parent_name) {
        return Err(HistoryError::contract(format!(
            "leaf {}: parent name {parent_name:?} is not a safe identifier",
            leaf.leaf_name()
        )));
    }
    set_ddl_timeouts(connection, statement_timeout_ms).await?;
    let detach = sqlx::query(&format!(
        "ALTER TABLE {parent_name} DETACH PARTITION {} {}",
        leaf.leaf_name(),
        step.keyword()
    ))
    .execute(&mut *connection)
    .await;
    match detach {
        Ok(_) => Ok(PhysicalDetach::Detached),
        Err(error) if is_lock_not_available(&error) => Ok(PhysicalDetach::Busy),
        Err(error) => Err(error.into()),
    }
}

/// Record the detach that this connection's detach statement completed, in
/// one READ COMMITTED transaction on the connection: `horsies_record_leaf_detached` and, when the
/// published manifest names the leaf, the reader publication commit
/// together. A failure rolls both back.
///
/// A relation that vanished after its detach is `LeafInspection::Missing`. A
/// leaf still attached, or a catalog row that vanished, contradicts the
/// detach and is `HistoryError::Contract`.
async fn record_detach<P: LoaderPublication>(
    connection: &mut PgConnection,
    leaf: &LeafRef,
    eligible_at: DateTime<Utc>,
    publisher: &P,
) -> Result<RetentionDetach, HistoryError> {
    let mut transaction = connection.begin().await?;
    let recorded = match record_and_publish(&mut transaction, leaf.leaf_name(), publisher).await {
        Ok(recorded) => recorded,
        Err(error) => {
            // The first error is the one to report. A rollback that fails
            // leaves a broken session, which the timeout restore of the
            // caller then fails on, so the connection is closed.
            let _rollback = transaction.rollback().await;
            return Err(error);
        }
    };
    transaction.commit().await?;
    match recorded_detach(leaf.leaf_name(), recorded)? {
        RecordedDetach::Detached => Ok(RetentionDetach::Recorded { eligible_at }),
        RecordedDetach::Missing => Ok(RetentionDetach::Inspection(LeafInspection::Missing {
            leaf_name: leaf.leaf_name().to_owned(),
            cataloged: true,
            eligible_at: Some(eligible_at),
        })),
    }
}

/// The record statement and, when it says so, the publication, inside the
/// caller's transaction. It does not commit.
async fn record_and_publish<P: LoaderPublication>(
    connection: &mut PgConnection,
    leaf_name: &str,
    publisher: &P,
) -> Result<DetachRecord, HistoryError> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut *connection)
        .await?;
    let recorded = record_leaf_detached(connection, leaf_name).await?;
    match recorded.manifest_names_leaf {
        true => {
            let _republished = publisher.republish(connection).await?;
        }
        false => {}
    }
    Ok(recorded.record)
}

/// Drop one already-detached PERIOD or HEARTBEAT leaf inside the caller's
/// transaction: one call of `horsies_drop_detached_leaf`, which reads the
/// published manifest itself. Refused while the published readers still
/// probe the relation. The catalog keeps the dropped leaf's row. It does not
/// commit; see `retirement::drop_detached_retention_leaf`.
pub async fn drop_detached_leaf(
    connection: &mut PgConnection,
    command: &DropDetachedHistoryLeaf,
) -> Result<LeafDrop, HistoryError> {
    drop_detached_retention_leaf(connection, command.leaf()).await
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

/// Restore the session timeouts read before the detach statement (`None`
/// when they were not changed) and release the leaf's session lock. Every
/// step runs; the first error is returned.
async fn restore_timeouts_and_unlock(
    connection: &mut PgConnection,
    leaf: &LeafRef,
    prior: Option<&PriorTimeouts>,
) -> Result<(), HistoryError> {
    let restore_statement = match prior.and_then(|prior| prior.statement.as_deref()) {
        Some(prior_timeout) => sqlx::query("SELECT set_config('statement_timeout', $1, false)")
            .bind(prior_timeout)
            .execute(&mut *connection)
            .await
            .map(|_| ())
            .map_err(HistoryError::from),
        None => Ok(()),
    };
    let restore_lock = match prior {
        Some(prior) => sqlx::query("SELECT set_config('lock_timeout', $1, false)")
            .bind(&prior.lock)
            .execute(&mut *connection)
            .await
            .map(|_| ())
            .map_err(HistoryError::from),
        None => Ok(()),
    };
    let unlock = unlock_leaf_for_session(connection, leaf.class_key(), leaf.bounds().lower()).await;
    restore_statement.and(restore_lock).and(unlock)
}

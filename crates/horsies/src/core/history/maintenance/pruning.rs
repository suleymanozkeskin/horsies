//! Contained finalize-first pruning driver.

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::core::history::commands::{
    DetachExpiredHistoryLeaf, DropDetachedHistoryLeaf, FinalizeInterruptedLeafDetach,
    InspectHistoryLeaf, LeafBounds, LeafRef, DETACH_STATEMENT_TIMEOUT_MS,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::heartbeats::partitioning::{
    sweep_expired_heartbeat_leaves, HeartbeatLeafSwept,
};
use crate::core::history::names::{HEARTBEAT_CLASS_KEY, LEAF_CATALOG, RETENTION_CLASSES};
use crate::core::history::outcomes::{LeafDrop, LeafInspection};
use crate::core::history::partitions::manager::{
    detach_expired_leaf, drop_detached_leaf, finalize_interrupted_detach, inspect_leaf,
    DetachExpiredLeafOutcome, NoQuarantine,
};
use crate::core::history::partitions::publication::LoaderPublication;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryLeafSwept {
    pub leaf_name: String,
    pub class_key: String,
    pub detach: DetachExpiredLeafOutcome,
    pub drop: Option<LeafDrop>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrunePass {
    pub finalized_leaves: Vec<String>,
    pub heartbeat_swept: Vec<HeartbeatLeafSwept>,
    pub history_swept: Vec<HistoryLeafSwept>,
    pub refusals: Vec<String>,
    pub errors: Vec<String>,
}

impl PrunePass {
    pub fn detached_count(&self) -> usize {
        self.heartbeat_swept
            .iter()
            .filter(|entry| {
                matches!(
                    entry.detach,
                    DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. })
                )
            })
            .count()
            + self
                .history_swept
                .iter()
                .filter(|entry| {
                    matches!(
                        entry.detach,
                        DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. })
                    )
                })
                .count()
    }

    pub fn dropped_count(&self) -> usize {
        self.heartbeat_swept
            .iter()
            .filter(|entry| matches!(entry.drop, Some(LeafDrop::Dropped { .. })))
            .count()
            + self
                .history_swept
                .iter()
                .filter(|entry| matches!(entry.drop, Some(LeafDrop::Dropped { .. })))
                .count()
    }

    pub fn acted(&self) -> bool {
        !self.finalized_leaves.is_empty()
            || !self.heartbeat_swept.is_empty()
            || !self.history_swept.is_empty()
            || !self.refusals.is_empty()
            || !self.errors.is_empty()
    }
}

const EXPIRED_HISTORY_FILTER: &str = "c.class_key <> $1 AND r.duration IS NOT NULL";
const EXPIRED_FINITE_FILTER: &str = "r.duration IS NOT NULL";

async fn expired_candidates(
    pool: &PgPool,
    filter: &str,
    bind_heartbeat: bool,
) -> Result<Vec<LeafRef>, HistoryError> {
    let sql = format!(
        "SELECT c.leaf_name, c.class_key, c.lower_anchor, c.upper_anchor
         FROM {LEAF_CATALOG} AS c JOIN {RETENTION_CLASSES} AS r
           ON r.class_key = c.class_key
         WHERE {filter} AND c.dropped_at IS NULL
           AND c.upper_anchor + r.duration <= statement_timestamp()
         ORDER BY c.lower_anchor"
    );
    let rows: Vec<(String, String, DateTime<Utc>, DateTime<Utc>)> = if bind_heartbeat {
        sqlx::query_as(&sql)
            .bind(HEARTBEAT_CLASS_KEY)
            .fetch_all(pool)
            .await?
    } else {
        sqlx::query_as(&sql).fetch_all(pool).await?
    };
    rows.into_iter()
        .map(|(name, class, lower, upper)| {
            let bounds = LeafBounds::new(lower, upper)
                .map_err(|error| HistoryError::contract(error.to_string()))?;
            LeafRef::new(name, class, bounds)
                .map_err(|error| HistoryError::contract(error.to_string()))
        })
        .collect()
}

async fn finalize_interrupted_detaches<P: LoaderPublication>(
    pool: &PgPool,
    publisher: &P,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let candidates = match expired_candidates(pool, EXPIRED_FINITE_FILTER, false).await {
        Ok(candidates) => candidates,
        Err(error) => {
            return (
                Vec::new(),
                Vec::new(),
                vec![format!("discover finalize: {error}")],
            )
        }
    };
    let mut interrupted = Vec::new();
    match pool.acquire().await {
        Ok(mut connection) => {
            for leaf in candidates {
                match inspect_leaf(&mut connection, &InspectHistoryLeaf::new(leaf.clone())).await {
                    Ok(LeafInspection::DetachInterrupted { .. }) => interrupted.push(leaf),
                    Ok(_) => {}
                    Err(error) => {
                        return (
                            Vec::new(),
                            Vec::new(),
                            vec![format!("inspect {}: {error}", leaf.leaf_name())],
                        );
                    }
                }
            }
        }
        Err(error) => return (Vec::new(), Vec::new(), vec![format!("connect: {error}")]),
    }
    let mut finalized = Vec::new();
    let mut refusals = Vec::new();
    let mut errors = Vec::new();
    for leaf in interrupted {
        let command = match FinalizeInterruptedLeafDetach::new(
            leaf.clone(),
            Some(DETACH_STATEMENT_TIMEOUT_MS),
        ) {
            Ok(command) => command,
            Err(error) => {
                errors.push(format!("finalize {}: {error}", leaf.leaf_name()));
                continue;
            }
        };
        match finalize_interrupted_detach(pool, &command, publisher).await {
            Ok(LeafInspection::Detached { .. }) => finalized.push(leaf.leaf_name().to_owned()),
            Ok(outcome) => refusals.push(format!("finalize {}: {outcome:?}", leaf.leaf_name())),
            Err(error) => errors.push(format!("finalize {}: {error}", leaf.leaf_name())),
        }
    }
    (finalized, refusals, errors)
}

pub async fn sweep_expired_history_leaves<P: LoaderPublication>(
    pool: &PgPool,
    publisher: &P,
) -> (Vec<HistoryLeafSwept>, Vec<String>) {
    let candidates = match expired_candidates(pool, EXPIRED_HISTORY_FILTER, true).await {
        Ok(candidates) => candidates,
        Err(error) => return (Vec::new(), vec![format!("discover history: {error}")]),
    };
    let mut swept = Vec::new();
    let mut errors = Vec::new();
    for leaf in candidates {
        let result: Result<HistoryLeafSwept, HistoryError> = async {
            let detach_command = DetachExpiredHistoryLeaf::new(
                leaf.clone(),
                None,
                Some(DETACH_STATEMENT_TIMEOUT_MS),
            )
            .map_err(|error| HistoryError::contract(error.to_string()))?;
            let detach =
                detach_expired_leaf(pool, &detach_command, publisher, &NoQuarantine).await?;
            let initially_detached = matches!(
                &detach,
                DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. })
            );
            let detached = if initially_detached {
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
            Ok(HistoryLeafSwept {
                leaf_name: leaf.leaf_name().to_owned(),
                class_key: leaf.class_key().to_owned(),
                detach,
                drop,
            })
        }
        .await;
        match result {
            Ok(entry) => swept.push(entry),
            Err(error) => errors.push(format!("{}: {error}", leaf.leaf_name())),
        }
    }
    (swept, errors)
}

pub async fn prune_expired_partitions<P: LoaderPublication>(
    pool: &PgPool,
    publisher: &P,
) -> PrunePass {
    let (finalized_leaves, mut refusals, mut errors) =
        finalize_interrupted_detaches(pool, publisher).await;
    let heartbeat_swept = match sweep_expired_heartbeat_leaves(pool, publisher).await {
        Ok(entries) => entries,
        Err(error) => {
            errors.push(format!("heartbeat sweep: {error}"));
            Vec::new()
        }
    };
    let (history_swept, history_errors) = sweep_expired_history_leaves(pool, publisher).await;
    errors.extend(history_errors);
    for entry in heartbeat_swept
        .iter()
        .map(|entry| (entry.leaf_name.as_str(), &entry.detach, entry.drop.as_ref()))
        .chain(
            history_swept
                .iter()
                .map(|entry| (entry.leaf_name.as_str(), &entry.detach, entry.drop.as_ref())),
        )
    {
        match entry {
            (_, _, Some(LeafDrop::Dropped { .. })) => {}
            (leaf, _, Some(drop)) => refusals.push(format!("{leaf}: {drop:?}")),
            (leaf, detach, None) => refusals.push(format!("{leaf}: {detach:?}")),
        }
    }
    PrunePass {
        finalized_leaves,
        heartbeat_swept,
        history_swept,
        refusals,
        errors,
    }
}

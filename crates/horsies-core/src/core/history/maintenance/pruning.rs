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
    DetachExpiredLeafOutcome, FinalizeInterruptedLeafOutcome, NoQuarantine,
};
use crate::core::history::partitions::publication::LoaderPublication;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryLeafSwept {
    pub leaf_name: String,
    pub class_key: String,
    pub detach: DetachExpiredLeafOutcome,
    pub drop: Option<LeafDrop>,
}

/// Expired leaves one candidate query of a pass reads at most.
pub const MAX_PRUNE_CANDIDATES_PER_PASS: usize = 512;
/// Leaves one pass acts on at most, heartbeat leaves first. A refused leaf
/// does not count; see `sweep_counts_toward_bound`.
pub const MAX_PRUNE_LEAVES_PER_PASS: usize = 64;

/// One sweep over the expired leaves of one kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafSweep<E> {
    pub swept: Vec<E>,
    /// One entry per leaf whose sweep failed.
    pub errors: Vec<String>,
    /// Leaves that count toward the per-pass bound.
    pub acted: usize,
    pub reach: PruneReach,
}

/// Whether one swept leaf counts toward `MAX_PRUNE_LEAVES_PER_PASS`: the leaf
/// ends detached or dropped. A busy, blocked, not-expired or quarantine-refused
/// leaf does not count. A detached leaf whose drop is refused counts, because
/// the detach outcome does not tell a detach of this pass from an earlier one.
pub(crate) fn sweep_counts_toward_bound(
    detach: &DetachExpiredLeafOutcome,
    drop: Option<&LeafDrop>,
) -> bool {
    match (detach, drop) {
        (_, Some(LeafDrop::Dropped { .. })) => true,
        (DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. }), _) => true,
        (DetachExpiredLeafOutcome::Inspection(_), _)
        | (DetachExpiredLeafOutcome::Busy { .. }, _)
        | (DetachExpiredLeafOutcome::QuarantineRefused(_), _) => false,
    }
}

/// Whether a per-pass bound stopped a pass. Errors and refusals are reported
/// apart from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneReach {
    /// No bound stopped the pass: every expired leaf found was a candidate.
    WithinBounds,
    /// A per-pass bound stopped the pass; the expired leaves left over are the
    /// next pass's candidates.
    Truncated,
}

/// One pruning pass. A leaf's error or refusal is reported and the pass goes
/// on with the other leaves; a defect is not caught.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrunePass {
    pub finalized_leaves: Vec<String>,
    pub heartbeat_swept: Vec<HeartbeatLeafSwept>,
    pub history_swept: Vec<HistoryLeafSwept>,
    pub refusals: Vec<String>,
    pub errors: Vec<String>,
    pub reach: PruneReach,
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

const EXPIRED_HISTORY_FILTER: &str = "c.class_key <> $1 AND r.class_kind = 'PERIOD'";
const EXPIRED_FINITE_FILTER: &str = "r.class_kind IN ('PERIOD', 'HEARTBEAT')";

/// How long a leaf of the joined class `r` stays after its upper bound. NULL
/// for `forever`, so a forever leaf never expires.
const CLASS_RETENTION_SQL: &str = "CASE r.class_kind
             WHEN 'PERIOD' THEN make_interval(days => r.retention_days)
             WHEN 'HEARTBEAT' THEN make_interval(hours => r.heartbeat_horizon_hours)
         END";

/// The oldest expired leaves that `filter` selects, at most
/// `MAX_PRUNE_CANDIDATES_PER_PASS`, and whether more remain.
async fn expired_candidates(
    pool: &PgPool,
    filter: &str,
    bind_heartbeat: bool,
) -> Result<(Vec<LeafRef>, PruneReach), HistoryError> {
    let sql = format!(
        "SELECT c.leaf_name, c.class_key, c.lower_anchor, c.upper_anchor
         FROM {LEAF_CATALOG} AS c JOIN {RETENTION_CLASSES} AS r
           ON r.class_key = c.class_key
         WHERE {filter} AND c.dropped_at IS NULL
           AND c.upper_anchor + {CLASS_RETENTION_SQL} <= statement_timestamp()
         ORDER BY c.lower_anchor
         LIMIT {}",
        MAX_PRUNE_CANDIDATES_PER_PASS + 1
    );
    let rows: Vec<(String, String, DateTime<Utc>, DateTime<Utc>)> = if bind_heartbeat {
        sqlx::query_as(&sql)
            .bind(HEARTBEAT_CLASS_KEY)
            .fetch_all(pool)
            .await?
    } else {
        sqlx::query_as(&sql).fetch_all(pool).await?
    };
    let reach = match rows.len() > MAX_PRUNE_CANDIDATES_PER_PASS {
        true => PruneReach::Truncated,
        false => PruneReach::WithinBounds,
    };
    let leaves = rows
        .into_iter()
        .take(MAX_PRUNE_CANDIDATES_PER_PASS)
        .map(|(name, class, lower, upper)| {
            let bounds = LeafBounds::new(lower, upper)
                .map_err(|error| HistoryError::contract(error.to_string()))?;
            LeafRef::new(name, class, bounds)
                .map_err(|error| HistoryError::contract(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((leaves, reach))
}

async fn finalize_interrupted_detaches<P: LoaderPublication>(
    pool: &PgPool,
    publisher: &P,
) -> (Vec<String>, Vec<String>, Vec<String>, PruneReach) {
    let (candidates, reach) = match expired_candidates(pool, EXPIRED_FINITE_FILTER, false).await {
        Ok(candidates) => candidates,
        Err(error) => {
            return (
                Vec::new(),
                Vec::new(),
                vec![format!("discover finalize: {error}")],
                PruneReach::WithinBounds,
            )
        }
    };
    let mut interrupted = Vec::new();
    let mut errors = Vec::new();
    match pool.acquire().await {
        Ok(mut connection) => {
            for leaf in candidates {
                match inspect_leaf(&mut connection, &InspectHistoryLeaf::new(leaf.clone())).await {
                    Ok(LeafInspection::DetachInterrupted { .. }) => interrupted.push(leaf),
                    Ok(_) => {}
                    Err(error) => errors.push(format!("inspect {}: {error}", leaf.leaf_name())),
                }
            }
        }
        Err(error) => {
            return (
                Vec::new(),
                Vec::new(),
                vec![format!("connect: {error}")],
                reach,
            )
        }
    }
    let mut finalized = Vec::new();
    let mut refusals = Vec::new();
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
            Ok(FinalizeInterruptedLeafOutcome::Inspection(LeafInspection::Detached { .. })) => {
                finalized.push(leaf.leaf_name().to_owned());
            }
            Ok(outcome) => refusals.push(format!("finalize {}: {outcome:?}", leaf.leaf_name())),
            Err(error) => errors.push(format!("finalize {}: {error}", leaf.leaf_name())),
        }
    }
    (finalized, refusals, errors, reach)
}

/// Detach one expired period-class leaf and drop it once detached. A leaf
/// left detached by an earlier crash is dropped too.
async fn sweep_history_leaf<P: LoaderPublication>(
    pool: &PgPool,
    leaf: &LeafRef,
    publisher: &P,
) -> Result<HistoryLeafSwept, HistoryError> {
    let detach_command =
        DetachExpiredHistoryLeaf::new(leaf.clone(), None, Some(DETACH_STATEMENT_TIMEOUT_MS))
            .map_err(|error| HistoryError::contract(error.to_string()))?;
    let detach = detach_expired_leaf(pool, &detach_command, publisher, &NoQuarantine).await?;
    let detached = match &detach {
        DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. }) => true,
        _ => {
            let mut connection = pool.acquire().await?;
            matches!(
                inspect_leaf(&mut connection, &InspectHistoryLeaf::new(leaf.clone())).await?,
                LeafInspection::Detached { .. }
            )
        }
    };
    let drop = match detached {
        true => {
            let mut transaction = pool.begin().await?;
            let outcome = drop_detached_leaf(
                &mut transaction,
                &DropDetachedHistoryLeaf::new(leaf.clone()),
                publisher,
            )
            .await?;
            transaction.commit().await?;
            Some(outcome)
        }
        false => None,
    };
    Ok(HistoryLeafSwept {
        leaf_name: leaf.leaf_name().to_owned(),
        class_key: leaf.class_key().to_owned(),
        detach,
        drop,
    })
}

/// Detach and drop expired period-class leaves, oldest first, until
/// `max_acted_leaves` leaves count toward the bound. A leaf whose sweep fails
/// counts, since its effects are unknown; its error goes to the error list and
/// the sweep goes on with the next leaf. A failed discovery sweeps nothing.
/// `Truncated` when expired leaves remain for the next pass.
pub async fn sweep_expired_history_leaves<P: LoaderPublication>(
    pool: &PgPool,
    publisher: &P,
    max_acted_leaves: usize,
) -> LeafSweep<HistoryLeafSwept> {
    let (candidates, candidate_reach) =
        match expired_candidates(pool, EXPIRED_HISTORY_FILTER, true).await {
            Ok(candidates) => candidates,
            Err(error) => {
                return LeafSweep {
                    swept: Vec::new(),
                    errors: vec![format!("discover history: {error}")],
                    acted: 0,
                    reach: PruneReach::WithinBounds,
                }
            }
        };
    let mut swept = Vec::new();
    let mut errors = Vec::new();
    let mut acted = 0;
    let mut reach = candidate_reach;
    for leaf in candidates {
        if acted >= max_acted_leaves {
            reach = PruneReach::Truncated;
            break;
        }
        match sweep_history_leaf(pool, &leaf, publisher).await {
            Ok(entry) => {
                acted += usize::from(sweep_counts_toward_bound(
                    &entry.detach,
                    entry.drop.as_ref(),
                ));
                swept.push(entry);
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

/// Prune expired partitions through a direct or session-capable pool.
pub async fn prune_expired_partitions<P: LoaderPublication>(
    pool: &PgPool,
    publisher: &P,
) -> PrunePass {
    let (finalized_leaves, mut refusals, mut errors, finalize_reach) =
        finalize_interrupted_detaches(pool, publisher).await;
    let heartbeat =
        match sweep_expired_heartbeat_leaves(pool, publisher, MAX_PRUNE_LEAVES_PER_PASS).await {
            Ok(sweep) => sweep,
            Err(error) => {
                errors.push(format!("discover heartbeat: {error}"));
                LeafSweep {
                    swept: Vec::new(),
                    errors: Vec::new(),
                    acted: 0,
                    reach: PruneReach::WithinBounds,
                }
            }
        };
    errors.extend(
        heartbeat
            .errors
            .into_iter()
            .map(|error| format!("heartbeat {error}")),
    );
    let heartbeat_swept = heartbeat.swept;
    let heartbeat_reach = heartbeat.reach;
    let history_budget = MAX_PRUNE_LEAVES_PER_PASS.saturating_sub(heartbeat.acted);
    let history = sweep_expired_history_leaves(pool, publisher, history_budget).await;
    errors.extend(history.errors);
    let history_swept = history.swept;
    let history_reach = history.reach;
    let reach = match (finalize_reach, heartbeat_reach, history_reach) {
        (PruneReach::WithinBounds, PruneReach::WithinBounds, PruneReach::WithinBounds) => {
            PruneReach::WithinBounds
        }
        _ => PruneReach::Truncated,
    };
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
        reach,
    }
}

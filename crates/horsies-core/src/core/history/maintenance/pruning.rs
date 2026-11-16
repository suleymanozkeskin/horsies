//! Contained finalize-first pruning driver.

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::core::history::commands::{
    DetachEmptyForeverLeaf, DetachExpiredHistoryLeaf, DropDetachedHistoryLeaf,
    FinalizeInterruptedLeafDetach, InspectHistoryLeaf, LeafBounds, LeafRef,
    DETACH_STATEMENT_TIMEOUT_MS,
};
use crate::core::history::cutover::state::cutover_complete;
use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;
use crate::core::history::errors::HistoryError;
use crate::core::history::heartbeats::partitioning::{
    sweep_expired_heartbeat_leaves, HeartbeatLeafSwept,
};
use crate::core::history::maintenance::gate::{
    active_maintenance_session, MaintenanceSessionError,
};
use crate::core::history::names::{HEARTBEAT_CLASS_KEY, LEAF_CATALOG, RETENTION_CLASSES};
use crate::core::history::outcomes::{LeafDrop, LeafInspection};
use crate::core::history::partitions::forever::FOREVER_LEGACY_LEAF;
use crate::core::history::partitions::manager::{
    detach_empty_forever_leaf, detach_expired_leaf, drop_detached_leaf,
    finalize_interrupted_detach, inspect_leaf, DetachExpiredLeafOutcome,
    FinalizeInterruptedLeafOutcome, NoQuarantine,
};
use crate::core::history::partitions::publication::LoaderPublication;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryLeafSwept {
    pub leaf_name: String,
    pub class_key: String,
    pub detach: DetachExpiredLeafOutcome,
    pub drop: Option<LeafDrop>,
}

/// What the sweep did with one closed daily `forever` leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForeverLeafSweepOutcome {
    Dropped,
    /// The leaf is still present; `detail` names the state or the refusal.
    Refused {
        detail: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeverLeafSwept {
    pub leaf_name: String,
    pub outcome: ForeverLeafSweepOutcome,
}

/// Why a pass did not sweep closed daily `forever` leaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeverSweepSkipped {
    /// Cutover relocation can still insert legacy rows into closed leaves.
    CutoverNotComplete,
    /// Transcode can still write replacement partitions.
    ArchiveMaintenanceActive,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ForeverSweepPass {
    pub swept: Vec<ForeverLeafSwept>,
    /// Closed leaves that hold rows. Never detached; not an action.
    pub kept_non_empty: Vec<String>,
    pub skipped: Option<ForeverSweepSkipped>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrunePass {
    pub finalized_leaves: Vec<String>,
    pub heartbeat_swept: Vec<HeartbeatLeafSwept>,
    pub history_swept: Vec<HistoryLeafSwept>,
    pub forever_swept: Vec<ForeverLeafSwept>,
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
            + self
                .forever_swept
                .iter()
                .filter(|entry| entry.outcome == ForeverLeafSweepOutcome::Dropped)
                .count()
    }

    pub fn acted(&self) -> bool {
        !self.finalized_leaves.is_empty()
            || !self.heartbeat_swept.is_empty()
            || !self.history_swept.is_empty()
            || !self.forever_swept.is_empty()
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
            Ok(FinalizeInterruptedLeafOutcome::Inspection(LeafInspection::Detached { .. })) => {
                finalized.push(leaf.leaf_name().to_owned());
            }
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

/// Detaches per pass. Candidates are not bounded by it, so non-empty old
/// leaves cannot hide empty ones behind them.
pub const MAX_FOREVER_DETACHES_PER_PASS: usize = 32;
/// Upper bound of the candidate list (closed daily forever leaves).
const MAX_FOREVER_SWEEP_CANDIDATES: i64 = 4_096;

enum ForeverCandidateAction {
    Detach(LeafRef),
    Finalize(LeafRef),
    Drop(LeafRef),
}

async fn closed_forever_candidates(pool: &PgPool) -> Result<Vec<LeafRef>, HistoryError> {
    let sql = format!(
        "SELECT c.leaf_name, c.lower_anchor, c.upper_anchor
         FROM {LEAF_CATALOG} AS c
         WHERE c.class_key = $1
           AND c.dropped_at IS NULL
           AND c.leaf_name <> $2
           AND c.upper_anchor - c.lower_anchor = interval '1 day'
           AND c.upper_anchor <= date_trunc('day', statement_timestamp(), 'UTC')
         ORDER BY c.lower_anchor
         LIMIT $3"
    );
    let rows: Vec<(String, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(&sql)
        .bind(FOREVER_CLASS_KEY)
        .bind(FOREVER_LEGACY_LEAF)
        .bind(MAX_FOREVER_SWEEP_CANDIDATES)
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|(name, lower, upper)| {
            let bounds = LeafBounds::new(lower, upper)
                .map_err(|error| HistoryError::contract(error.to_string()))?;
            LeafRef::new(name, FOREVER_CLASS_KEY, bounds)
                .map_err(|error| HistoryError::contract(error.to_string()))
        })
        .collect()
}

async fn forever_sweep_skipped(pool: &PgPool) -> Result<Option<ForeverSweepSkipped>, HistoryError> {
    let mut connection = pool.acquire().await?;
    if !cutover_complete(&mut connection).await? {
        return Ok(Some(ForeverSweepSkipped::CutoverNotComplete));
    }
    match active_maintenance_session(&mut connection).await {
        Ok(None) => Ok(None),
        Ok(Some(_)) | Err(MaintenanceSessionError::MultipleActive) => {
            Ok(Some(ForeverSweepSkipped::ArchiveMaintenanceActive))
        }
        Err(error) => Err(HistoryError::contract(error.to_string())),
    }
}

/// Inspects every candidate on one connection; empty eligible leaves become
/// detaches, interrupted detaches become finalizations, detached leaves drops.
async fn plan_forever_sweep(
    pool: &PgPool,
    pass: &mut ForeverSweepPass,
) -> Result<Vec<ForeverCandidateAction>, HistoryError> {
    let candidates = closed_forever_candidates(pool).await?;
    let mut connection = pool.acquire().await?;
    let mut actions = Vec::new();
    for leaf in candidates {
        let inspection = InspectHistoryLeaf::closed_empty_forever(leaf.clone())
            .map_err(|error| HistoryError::contract(error.to_string()))?;
        match inspect_leaf(&mut connection, &inspection).await? {
            LeafInspection::KeptNonEmpty { leaf_name } => pass.kept_non_empty.push(leaf_name),
            LeafInspection::Detachable { .. } => actions.push(ForeverCandidateAction::Detach(leaf)),
            LeafInspection::DetachInterrupted { .. } => {
                actions.push(ForeverCandidateAction::Finalize(leaf));
            }
            LeafInspection::Detached { .. } => actions.push(ForeverCandidateAction::Drop(leaf)),
            other => pass.swept.push(ForeverLeafSwept {
                leaf_name: leaf.leaf_name().to_owned(),
                outcome: ForeverLeafSweepOutcome::Refused {
                    detail: format!("{other:?}"),
                },
            }),
        }
    }
    Ok(actions)
}

async fn drop_closed_forever_leaf<P: LoaderPublication>(
    pool: &PgPool,
    leaf: &LeafRef,
    publisher: &P,
) -> Result<ForeverLeafSweepOutcome, HistoryError> {
    let command = DropDetachedHistoryLeaf::closed_empty_forever(leaf.clone())
        .map_err(|error| HistoryError::contract(error.to_string()))?;
    let mut transaction = pool.begin().await?;
    let drop = drop_detached_leaf(&mut transaction, &command, publisher).await?;
    transaction.commit().await?;
    Ok(match drop {
        LeafDrop::Dropped { .. } => ForeverLeafSweepOutcome::Dropped,
        other => ForeverLeafSweepOutcome::Refused {
            detail: format!("{other:?}"),
        },
    })
}

async fn act_on_forever_leaf<P: LoaderPublication>(
    pool: &PgPool,
    action: ForeverCandidateAction,
    publisher: &P,
) -> Result<(String, ForeverLeafSweepOutcome, bool), HistoryError> {
    match action {
        ForeverCandidateAction::Detach(leaf) => {
            let command =
                DetachEmptyForeverLeaf::new(leaf.clone(), Some(DETACH_STATEMENT_TIMEOUT_MS))
                    .map_err(|error| HistoryError::contract(error.to_string()))?;
            match detach_empty_forever_leaf(pool, &command, publisher).await? {
                DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. }) => {
                    let outcome = drop_closed_forever_leaf(pool, &leaf, publisher).await?;
                    Ok((leaf.leaf_name().to_owned(), outcome, true))
                }
                other => Ok((
                    leaf.leaf_name().to_owned(),
                    ForeverLeafSweepOutcome::Refused {
                        detail: format!("{other:?}"),
                    },
                    false,
                )),
            }
        }
        ForeverCandidateAction::Finalize(leaf) => {
            let command = FinalizeInterruptedLeafDetach::closed_empty_forever(
                leaf.clone(),
                Some(DETACH_STATEMENT_TIMEOUT_MS),
            )
            .map_err(|error| HistoryError::contract(error.to_string()))?;
            match finalize_interrupted_detach(pool, &command, publisher).await? {
                FinalizeInterruptedLeafOutcome::Inspection(LeafInspection::Detached { .. }) => {
                    let outcome = drop_closed_forever_leaf(pool, &leaf, publisher).await?;
                    Ok((leaf.leaf_name().to_owned(), outcome, false))
                }
                other => Ok((
                    leaf.leaf_name().to_owned(),
                    ForeverLeafSweepOutcome::Refused {
                        detail: format!("{other:?}"),
                    },
                    false,
                )),
            }
        }
        ForeverCandidateAction::Drop(leaf) => {
            let outcome = drop_closed_forever_leaf(pool, &leaf, publisher).await?;
            Ok((leaf.leaf_name().to_owned(), outcome, false))
        }
    }
}

/// Detaches and drops closed daily `forever` leaves that hold no rows, and
/// finishes interrupted detaches of such leaves. At most
/// `MAX_FOREVER_DETACHES_PER_PASS` new detaches per pass. Skipped while the
/// cutover is not complete or archive maintenance is active.
///
/// `pool` must preserve PostgreSQL session affinity.
pub async fn sweep_empty_forever_leaves<P: LoaderPublication>(
    pool: &PgPool,
    publisher: &P,
) -> ForeverSweepPass {
    let mut pass = ForeverSweepPass::default();
    match forever_sweep_skipped(pool).await {
        Ok(None) => {}
        Ok(Some(skipped)) => {
            pass.skipped = Some(skipped);
            return pass;
        }
        Err(error) => {
            pass.errors.push(format!("forever sweep gate: {error}"));
            return pass;
        }
    }
    let actions = match plan_forever_sweep(pool, &mut pass).await {
        Ok(actions) => actions,
        Err(error) => {
            pass.errors
                .push(format!("forever sweep discovery: {error}"));
            return pass;
        }
    };
    let mut detaches = 0_usize;
    for action in actions {
        if matches!(action, ForeverCandidateAction::Detach(_))
            && detaches >= MAX_FOREVER_DETACHES_PER_PASS
        {
            continue;
        }
        let leaf_name = match &action {
            ForeverCandidateAction::Detach(leaf)
            | ForeverCandidateAction::Finalize(leaf)
            | ForeverCandidateAction::Drop(leaf) => leaf.leaf_name().to_owned(),
        };
        match act_on_forever_leaf(pool, action, publisher).await {
            Ok((leaf_name, outcome, detached)) => {
                detaches += usize::from(detached);
                pass.swept.push(ForeverLeafSwept { leaf_name, outcome });
            }
            Err(error) => pass.errors.push(format!("{leaf_name}: {error}")),
        }
    }
    pass
}

/// Prune expired partitions through a direct or session-capable pool.
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
    let forever_pass = sweep_empty_forever_leaves(pool, publisher).await;
    errors.extend(forever_pass.errors);
    for entry in &forever_pass.swept {
        if let ForeverLeafSweepOutcome::Refused { detail } = &entry.outcome {
            refusals.push(format!("{}: {detail}", entry.leaf_name));
        }
    }
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
        forever_swept: forever_pass.swept,
        refusals,
        errors,
    }
}

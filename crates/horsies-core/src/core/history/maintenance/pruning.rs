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

/// Whether one swept leaf counts toward `MAX_PRUNE_LEAVES_PER_PASS`: this
/// pass detached it or dropped it. A busy, blocked, not-expired or
/// quarantine-refused leaf does not count, and neither does a leaf an earlier
/// pass detached whose drop is refused now.
pub(crate) fn sweep_counts_toward_bound(
    detach: &DetachExpiredLeafOutcome,
    drop: Option<&LeafDrop>,
) -> bool {
    match (detach, drop) {
        (DetachExpiredLeafOutcome::Detached { .. }, _) => true,
        (_, Some(LeafDrop::Dropped { .. })) => true,
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
            .filter(|entry| matches!(entry.detach, DetachExpiredLeafOutcome::Detached { .. }))
            .count()
            + self
                .history_swept
                .iter()
                .filter(|entry| matches!(entry.detach, DetachExpiredLeafOutcome::Detached { .. }))
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

/// Which expired leaves a candidate read of a pruning pass selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CandidateScope {
    /// Period-class leaves, for the history sweep. Python
    /// `EXPIRED_HISTORY_LEAVES_SQL` (`horsies/core/history/maintenance/pruning.py:77-87`).
    History,
    /// Period and heartbeat leaves, for the finalize pre-phase, which must see
    /// every parent. Python `EXPIRED_FINITE_LEAVES_SQL` (`pruning.py:92-106`).
    Finite,
}

impl CandidateScope {
    fn filter(self) -> &'static str {
        match self {
            Self::History => "c.class_key <> $1 AND r.class_kind = 'PERIOD'",
            Self::Finite => "r.class_kind IN ('PERIOD', 'HEARTBEAT')",
        }
    }
}

/// How long a leaf of the joined class `r` stays after its upper bound. NULL
/// for `forever`, so a forever leaf never expires.
const CLASS_RETENTION_SQL: &str = "CASE r.class_kind
             WHEN 'PERIOD' THEN make_interval(days => r.retention_days)
             WHEN 'HEARTBEAT' THEN make_interval(hours => r.heartbeat_horizon_hours)
         END";

/// The candidate read that a pruning pass sends for `scope`: leaves whose
/// class period has passed and that the catalog has not recorded as dropped,
/// oldest first, at most `MAX_PRUNE_CANDIDATES_PER_PASS + 1` rows (one more
/// than the bound shows that the pass is truncated). `History` takes `$1`, the
/// heartbeat class key. The `dropped_at IS NULL` predicate lets the read use
/// the partial index `horsies_task_history_leaf_catalog_active`.
pub(crate) fn expired_candidates_sql(scope: CandidateScope) -> String {
    format!(
        "SELECT c.leaf_name, c.class_key, c.lower_anchor, c.upper_anchor
         FROM {LEAF_CATALOG} AS c JOIN {RETENTION_CLASSES} AS r
           ON r.class_key = c.class_key
         WHERE {} AND c.dropped_at IS NULL
           AND c.upper_anchor + {CLASS_RETENTION_SQL} <= statement_timestamp()
         ORDER BY c.lower_anchor
         LIMIT {}",
        scope.filter(),
        MAX_PRUNE_CANDIDATES_PER_PASS + 1
    )
}

/// The oldest expired leaves of `scope`, at most
/// `MAX_PRUNE_CANDIDATES_PER_PASS`, and whether more remain.
async fn expired_candidates(
    pool: &PgPool,
    scope: CandidateScope,
) -> Result<(Vec<LeafRef>, PruneReach), HistoryError> {
    let sql = expired_candidates_sql(scope);
    let rows: Vec<(String, String, DateTime<Utc>, DateTime<Utc>)> = match scope {
        CandidateScope::History => {
            sqlx::query_as(&sql)
                .bind(HEARTBEAT_CLASS_KEY)
                .fetch_all(pool)
                .await?
        }
        CandidateScope::Finite => sqlx::query_as(&sql).fetch_all(pool).await?,
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
    let (candidates, reach) = match expired_candidates(pool, CandidateScope::Finite).await {
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
        DetachExpiredLeafOutcome::Detached { .. }
        | DetachExpiredLeafOutcome::Inspection(LeafInspection::Detached { .. }) => true,
        DetachExpiredLeafOutcome::Busy { .. }
        | DetachExpiredLeafOutcome::Inspection(_)
        | DetachExpiredLeafOutcome::QuarantineRefused(_) => {
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
        match expired_candidates(pool, CandidateScope::History).await {
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

#[cfg(test)]
mod tests {
    //! Python specification: `tests/integration/test_history_catalog_active_index.py`.

    use std::collections::BTreeSet;

    use serde_json::Value;
    use serial_test::serial;

    use super::{expired_candidates_sql, CandidateScope};
    use crate::core::history::names::{HEARTBEAT_CLASS_KEY, LEAF_CATALOG, RETENTION_CLASSES};

    const ACTIVE_INDEX: &str = "horsies_task_history_leaf_catalog_active";
    const CLASS_KEY: &str = "standard_30d";
    const DROPPED_RECORDS: i32 = 6_000;
    const ACTIVE_RECORDS: i32 = 20;
    /// Upper bound of the plan nodes the walk reads; a larger plan fails the
    /// test.
    const MAX_PLAN_NODES: usize = 256;

    /// Every node of the plan tree under `root`.
    fn plan_nodes(root: &Value) -> Vec<&Value> {
        let mut nodes = Vec::new();
        let mut pending = vec![root];
        while let Some(node) = pending.pop() {
            assert!(
                nodes.len() < MAX_PLAN_NODES,
                "plan has more than {MAX_PLAN_NODES} nodes"
            );
            nodes.push(node);
            if let Some(children) = node.get("Plans").and_then(Value::as_array) {
                pending.extend(children.iter());
            }
        }
        nodes
    }

    /// For each catalog scan of the plan, the indexes it reads through. A scan
    /// that reads the table without an index (a Seq Scan) reads through none.
    fn catalog_scan_indexes(root: &Value) -> Vec<BTreeSet<String>> {
        plan_nodes(root)
            .into_iter()
            .filter(|node| node.get("Relation Name").and_then(Value::as_str) == Some(LEAF_CATALOG))
            .map(|node| match node.get("Node Type").and_then(Value::as_str) {
                Some("Index Scan" | "Index Only Scan") => node
                    .get("Index Name")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .into_iter()
                    .collect(),
                Some("Bitmap Heap Scan") => plan_nodes(node)
                    .into_iter()
                    .filter_map(|inner| inner.get("Index Name").and_then(Value::as_str))
                    .map(str::to_owned)
                    .collect(),
                _ => BTreeSet::new(),
            })
            .collect()
    }

    /// `test_expired_leaf_scan_examines_only_active_records`: with 6,000
    /// dropped catalog records and 20 active ones of one class, analyzed,
    /// every catalog scan of each candidate read that a pruning pass sends
    /// reads through the active-record index, so the read examines no dropped
    /// record. Rows are not counted: EXPLAIN gives a scan's rows as a rounded
    /// mean per loop.
    #[tokio::test]
    #[serial]
    async fn the_candidate_reads_scan_only_active_catalog_records() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut transaction = pool.begin().await.expect("begin");
        sqlx::query(&format!(
            "INSERT INTO {RETENTION_CLASSES} (
                class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at
             ) VALUES ($1, 'PERIOD', 30, NULL, statement_timestamp())
             ON CONFLICT (class_key) DO NOTHING"
        ))
        .bind(CLASS_KEY)
        .execute(transaction.as_mut())
        .await
        .expect("register the class");
        sqlx::query(&format!(
            "INSERT INTO {LEAF_CATALOG} (
                leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor,
                index_schema_version, created_at, detached_at, dropped_at
             )
             SELECT 'catalog_probe_leaf_' || day, $1, 'AT',
                    TIMESTAMPTZ '2000-01-01 00:00:00+00' + day * interval '1 day',
                    TIMESTAMPTZ '2000-01-02 00:00:00+00' + day * interval '1 day',
                    1, NOW(),
                    CASE WHEN day < $2 THEN NOW() END,
                    CASE WHEN day < $2 THEN NOW() END
             FROM generate_series(0, $3 - 1) AS day"
        ))
        .bind(CLASS_KEY)
        .bind(DROPPED_RECORDS)
        .bind(DROPPED_RECORDS + ACTIVE_RECORDS)
        .execute(transaction.as_mut())
        .await
        .expect("seed the catalog");
        sqlx::query(&format!("ANALYZE {LEAF_CATALOG}"))
            .execute(transaction.as_mut())
            .await
            .expect("analyze the catalog");
        let definition: Option<String> =
            sqlx::query_scalar("SELECT pg_get_indexdef(to_regclass($1))")
                .bind(ACTIVE_INDEX)
                .fetch_one(transaction.as_mut())
                .await
                .expect("read the index definition");

        // The index holds only records whose leaf is not dropped.
        assert!(
            definition
                .as_deref()
                .is_some_and(|text| text.ends_with(" WHERE (dropped_at IS NULL)")),
            "{definition:?}"
        );
        for scope in [CandidateScope::History, CandidateScope::Finite] {
            let explain = format!(
                "EXPLAIN (ANALYZE, FORMAT JSON) {}",
                expired_candidates_sql(scope)
            );
            let plan: Value = match scope {
                CandidateScope::History => {
                    sqlx::query_scalar(&explain)
                        .bind(HEARTBEAT_CLASS_KEY)
                        .fetch_one(transaction.as_mut())
                        .await
                }
                CandidateScope::Finite => {
                    sqlx::query_scalar(&explain)
                        .fetch_one(transaction.as_mut())
                        .await
                }
            }
            .expect("explain the candidate read");
            let root = &plan[0]["Plan"];
            let scans = catalog_scan_indexes(root);
            assert!(!scans.is_empty(), "{scope:?}: {plan}");
            assert!(
                scans
                    .iter()
                    .all(|scan| scan.len() == 1 && scan.contains(ACTIVE_INDEX)),
                "{scope:?}: {plan}"
            );
            // The seeded active records are expired, so the read returns them.
            assert!(
                root["Actual Rows"].as_f64().expect("actual rows") >= f64::from(ACTIVE_RECORDS),
                "{scope:?}: {plan}"
            );
        }
        transaction.rollback().await.expect("rollback");
    }
}

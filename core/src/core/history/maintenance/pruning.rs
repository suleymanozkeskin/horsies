//! Contained finalize-first pruning driver, on the core pools.
//!
//! The pass report names each refusal by its outcome name
//! (`LeafInspection::outcome_name`).

use crate::broker::core_rows::StatementLog;
use crate::core::history::commands::{
    DetachExpiredHistoryLeaf, DropDetachedHistoryLeaf, FinalizeInterruptedLeafDetach,
    InspectHistoryLeaf, LeafRef, DETACH_STATEMENT_TIMEOUT_MS,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::heartbeats::partitioning::{sweep_heartbeat_leaves, HeartbeatLeafSwept};
use crate::core::history::maintenance::coverage::maintenance_connection;
use crate::core::history::outcomes::{LeafDrop, LeafInspection};
use crate::core::history::partitions::manager::{
    detach_expired_leaf, drop_detached_leaf_in_pool, finalize_interrupted_detach,
    inspect_leaf_in_pool, DetachExpiredLeafOutcome, FinalizeInterruptedLeafOutcome,
};
use crate::core::history::partitions::retirement::{
    read_prune_candidates, CandidateAttachment, PruneCandidates, RetiredClassKind, PRUNE_CANDIDATES,
};
use crate::db::CorePool;

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

/// Whether one swept leaf counts toward `MAX_PRUNE_LEAVES_PER_PASS` (Python
/// `counts_toward_prune_bound`): the leaf has no parent after its detach
/// (this pass detached it, or an earlier pass did), or this pass dropped it.
/// A busy, blocked or not-expired leaf does not count, so leaves that stay
/// refused at the old end of the catalog cannot hold back the newer expired
/// leaves.
pub(crate) fn sweep_counts_toward_bound(
    detach: &DetachExpiredLeafOutcome,
    drop: Option<&LeafDrop>,
) -> bool {
    match (detach.left_detached(), drop) {
        (true, _) | (false, Some(LeafDrop::Dropped { .. })) => true,
        (
            false,
            None
            | Some(
                LeafDrop::Busy { .. }
                | LeafDrop::RefusedLoaderReferences { .. }
                | LeafDrop::Inspection(_),
            ),
        ) => false,
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
    /// `leaf: Outcome` for each swept leaf that was not dropped, and
    /// `finalize leaf: Outcome` for each pending detach that did not end
    /// detached; `Outcome` is the class name of the Python outcome.
    pub refusals: Vec<String>,
    /// `leaf: cause`, `heartbeat leaf: cause` and `finalize leaf: cause` for
    /// each leaf whose step failed.
    pub errors: Vec<String>,
    pub reach: PruneReach,
}

impl PrunePass {
    /// The swept leaves that have no parent after their detach (Python
    /// `detached_count`): detached by this pass or by an earlier one.
    pub fn detached_count(&self) -> usize {
        self.heartbeat_swept
            .iter()
            .filter(|entry| entry.detach.left_detached())
            .count()
            + self
                .history_swept
                .iter()
                .filter(|entry| entry.detach.left_detached())
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

/// The candidates of one pass: one `horsies_prune_candidates` read of at most
/// `MAX_PRUNE_CANDIDATES_PER_PASS` leaves, on a connection of `pool`. Reads
/// only.
pub(crate) async fn pass_candidates(
    pool: &CorePool,
    log: StatementLog,
) -> Result<PruneCandidates, HistoryError> {
    let mut connection = maintenance_connection(pool, &PRUNE_CANDIDATES).await?;
    read_prune_candidates(&mut connection, log, MAX_PRUNE_CANDIDATES_PER_PASS).await
}

/// `Truncated` when any part of a pass was truncated.
fn combined_reach(reaches: &[PruneReach]) -> PruneReach {
    match reaches
        .iter()
        .any(|reach| matches!(reach, PruneReach::Truncated))
    {
        true => PruneReach::Truncated,
        false => PruneReach::WithinBounds,
    }
}

/// The finalized leaves, refusals and errors of the finalize phase.
struct Finalized {
    leaves: Vec<String>,
    refusals: Vec<String>,
    errors: Vec<String>,
}

/// Finalize every candidate whose concurrent detach is pending
/// (`CandidateAttachment::DetachPending`), PERIOD and HEARTBEAT leaves alike.
///
/// A finalization that does not end detached is a refusal, for example a
/// blocked leaf that stays pending until its blockers drain. A database
/// error of one leaf is reported as `finalize leaf: cause`. Finalizations do
/// not count toward the acted-leaf bound.
async fn finalize_interrupted_detaches(
    pool: &CorePool,
    log: StatementLog,
    candidates: &PruneCandidates,
) -> Finalized {
    let mut finalized = Finalized {
        leaves: Vec::new(),
        refusals: Vec::new(),
        errors: Vec::new(),
    };
    for candidate in &candidates.leaves {
        match candidate.attachment {
            CandidateAttachment::DetachPending => {}
            CandidateAttachment::Attached
            | CandidateAttachment::Detached
            | CandidateAttachment::Missing => continue,
        }
        let leaf = &candidate.leaf;
        let command = match FinalizeInterruptedLeafDetach::new(
            leaf.clone(),
            Some(DETACH_STATEMENT_TIMEOUT_MS),
        ) {
            Ok(command) => command,
            Err(error) => {
                finalized
                    .errors
                    .push(format!("finalize {}: {error}", leaf.leaf_name()));
                continue;
            }
        };
        match finalize_interrupted_detach(pool, log, &command).await {
            Ok(FinalizeInterruptedLeafOutcome::Inspection(LeafInspection::Detached { .. })) => {
                finalized.leaves.push(leaf.leaf_name().to_owned());
            }
            Ok(outcome) => finalized.refusals.push(format!(
                "finalize {}: {}",
                leaf.leaf_name(),
                outcome.outcome_name()
            )),
            Err(error) => finalized
                .errors
                .push(format!("finalize {}: {error}", leaf.leaf_name())),
        }
    }
    finalized
}

/// Detach one expired PERIOD or HEARTBEAT leaf and drop it once detached
/// (Python `_sweep_history_leaf`). A leaf that an earlier crash left
/// detached is found by a second inspection and dropped too. The detach and
/// the drop each run on their own connection of `pool`; the drop commits
/// alone.
///
/// Errors: the first failed step's `HistoryError`; the steps before it stay
/// committed.
pub(crate) async fn sweep_retention_leaf(
    pool: &CorePool,
    log: StatementLog,
    leaf: &LeafRef,
) -> Result<(DetachExpiredLeafOutcome, Option<LeafDrop>), HistoryError> {
    let detach_command =
        DetachExpiredHistoryLeaf::new(leaf.clone(), Some(DETACH_STATEMENT_TIMEOUT_MS))
            .map_err(|error| HistoryError::contract(error.to_string()))?;
    let detach = detach_expired_leaf(pool, log, &detach_command).await?;
    let detached = match detach.left_detached() {
        true => true,
        // A crash between detach and drop leaves a detached leaf that this
        // sweep must still drop: inspect again for that state.
        false => matches!(
            inspect_leaf_in_pool(pool, log, &InspectHistoryLeaf::new(leaf.clone())).await?,
            LeafInspection::Detached { .. }
        ),
    };
    let drop = match detached {
        true => Some(
            drop_detached_leaf_in_pool(pool, log, &DropDetachedHistoryLeaf::new(leaf.clone()))
                .await?,
        ),
        false => None,
    };
    Ok((detach, drop))
}

/// Detach and drop the given expired period-class leaves, in the given
/// order, until `max_acted_leaves` leaves count toward the bound. A leaf
/// whose sweep fails counts, since its effects are unknown; its error goes to
/// the error list and the sweep goes on with the next leaf. `Truncated` when
/// the bound left leaves for the next pass.
pub(crate) async fn sweep_history_leaves(
    pool: &CorePool,
    log: StatementLog,
    leaves: &[LeafRef],
    max_acted_leaves: usize,
) -> LeafSweep<HistoryLeafSwept> {
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
                swept.push(HistoryLeafSwept {
                    leaf_name: leaf.leaf_name().to_owned(),
                    class_key: leaf.class_key().to_owned(),
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

/// Read the candidates of one pass and sweep its period-class leaves, oldest
/// first, until `max_acted_leaves` leaves count toward the bound. `Truncated`
/// when the candidate read or the leaf bound left expired leaves for the next
/// pass.
///
/// Errors: the failed candidate read (Python raises it); nothing was swept.
pub(crate) async fn sweep_expired_history_leaves(
    pool: &CorePool,
    log: StatementLog,
    max_acted_leaves: usize,
) -> Result<LeafSweep<HistoryLeafSwept>, HistoryError> {
    let candidates = pass_candidates(pool, log).await?;
    let sweep = sweep_history_leaves(
        pool,
        log,
        &candidates.of_kind(RetiredClassKind::Period),
        max_acted_leaves,
    )
    .await;
    Ok(LeafSweep {
        reach: combined_reach(&[candidates.reach, sweep.reach]),
        ..sweep
    })
}

/// One pruning pass on a direct or session-capable core pool (Python
/// `prune_expired_partitions`): one candidate read
/// (`horsies_prune_candidates`), then the finalization of the interrupted
/// detaches, then the heartbeat and the period-class sweeps. Every checkout
/// is reserved work.
///
/// The finalize phase runs first across every finite class, because one
/// pending finalization blocks all further detaches on its parent. The
/// sweeps act on at most `MAX_PRUNE_LEAVES_PER_PASS` leaves, heartbeat leaves
/// first. A database error of one leaf is reported and the pass goes on with
/// the next leaf; each leaf's detach and drop commit alone.
///
/// Errors: the failed candidate read (Python raises it); nothing was acted
/// on.
pub(crate) async fn prune_expired_partitions(
    pool: &CorePool,
    log: StatementLog,
) -> Result<PrunePass, HistoryError> {
    prune_expired_partitions_within(pool, log, MAX_PRUNE_LEAVES_PER_PASS).await
}

/// The pruning pass of [`prune_expired_partitions`] with `max_acted_leaves`
/// in place of `MAX_PRUNE_LEAVES_PER_PASS`: the heartbeat sweep acts first,
/// and the period-class sweep gets what the heartbeat sweep left of the
/// bound.
///
/// Errors: as [`prune_expired_partitions`].
pub(crate) async fn prune_expired_partitions_within(
    pool: &CorePool,
    log: StatementLog,
    max_acted_leaves: usize,
) -> Result<PrunePass, HistoryError> {
    let candidates = pass_candidates(pool, log).await?;
    let finalized = finalize_interrupted_detaches(pool, log, &candidates).await;
    let heartbeat = sweep_heartbeat_leaves(
        pool,
        log,
        &candidates.of_kind(RetiredClassKind::Heartbeat),
        max_acted_leaves,
    )
    .await;
    let history = sweep_history_leaves(
        pool,
        log,
        &candidates.of_kind(RetiredClassKind::Period),
        max_acted_leaves.saturating_sub(heartbeat.acted),
    )
    .await;
    let mut refusals = finalized.refusals;
    for entry in heartbeat
        .swept
        .iter()
        .map(|entry| (entry.leaf_name.as_str(), &entry.detach, entry.drop.as_ref()))
        .chain(
            history
                .swept
                .iter()
                .map(|entry| (entry.leaf_name.as_str(), &entry.detach, entry.drop.as_ref())),
        )
    {
        match entry {
            (_, _, Some(LeafDrop::Dropped { .. })) => {}
            (leaf, _, Some(drop)) => refusals.push(format!("{leaf}: {}", drop.outcome_name())),
            (leaf, detach, None) => refusals.push(format!("{leaf}: {}", detach.outcome_name())),
        }
    }
    let mut errors = finalized.errors;
    errors.extend(
        heartbeat
            .errors
            .into_iter()
            .map(|error| format!("heartbeat {error}")),
    );
    errors.extend(history.errors);
    Ok(PrunePass {
        finalized_leaves: finalized.leaves,
        heartbeat_swept: heartbeat.swept,
        history_swept: history.swept,
        refusals,
        errors,
        reach: combined_reach(&[candidates.reach, heartbeat.reach, history.reach]),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::PoisonError;

    use serde_json::Value;

    use super::MAX_PRUNE_CANDIDATES_PER_PASS;
    use crate::core::history::names::{LEAF_CATALOG, RETENTION_CLASSES};
    use crate::test_support::connect_collecting_notices;

    const ACTIVE_INDEX: &str = "horsies_task_history_leaf_catalog_active";
    const CLASS_KEY: &str = "standard_30d";
    const DROPPED_RECORDS: i32 = 6_000;
    const ACTIVE_RECORDS: i32 = 20;
    /// Upper bound of the plan nodes the walk reads; a larger plan fails the
    /// test.
    const MAX_PLAN_NODES: usize = 256;
    /// The text before the JSON plan in an `auto_explain` message.
    const PLAN_MARKER: &str = "plan:\n";
    /// The session settings that send the plan of each statement inside a
    /// function to the client as a notice.
    const AUTO_EXPLAIN_SETTINGS: &str = "LOAD 'auto_explain';
         SET LOCAL auto_explain.log_min_duration = 0;
         SET LOCAL auto_explain.log_nested_statements = on;
         SET LOCAL auto_explain.log_analyze = on;
         SET LOCAL auto_explain.log_format = 'json';
         SET LOCAL client_min_messages = log;";

    /// The bounds of one pass: 512 candidates per candidate read and 64
    /// acted leaves. Both are `usize` constants, so a pass always reads at
    /// least one candidate and a leaf bound is never negative.
    #[test]
    fn the_prune_bounds_are_the_values_of_a_pass() {
        assert_eq!(MAX_PRUNE_CANDIDATES_PER_PASS, 512);
        assert_eq!(super::MAX_PRUNE_LEAVES_PER_PASS, 64);
    }

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

    /// The root plan node of every `auto_explain` message.
    fn logged_plans(messages: &[String]) -> Vec<Value> {
        messages
            .iter()
            .filter_map(|message| message.split_once(PLAN_MARKER))
            .map(|(_, plan)| {
                let document: Value =
                    serde_json::from_str(plan).expect("an auto_explain plan is JSON");
                document["Plan"].clone()
            })
            .collect()
    }

    /// `test_expired_leaf_scan_examines_only_active_records`: with 6,000
    /// dropped catalog records and 20 active ones of one class, analyzed,
    /// every catalog scan of the statement inside `horsies_prune_candidates`
    /// reads through the active-record index, so the read examines no
    /// dropped record. A plain `EXPLAIN` of a PL/pgSQL call shows a Function
    /// Scan only; the plan of the statement inside comes from `auto_explain`
    /// with `log_nested_statements`, as a notice. Rows are not counted:
    /// EXPLAIN gives a scan's rows as a rounded mean per loop.
    #[tokio::test]
    async fn the_candidate_read_scans_only_active_catalog_records() {
        let url = crate::test_support::migrated_database_url().await;
        let (client, notices) = connect_collecting_notices(&url).await;
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .execute(
                &format!(
                    "INSERT INTO {RETENTION_CLASSES} (
                        class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at
                     ) VALUES ($1, 'PERIOD', 30, NULL, statement_timestamp())
                     ON CONFLICT (class_key) DO NOTHING"
                ),
                &[&CLASS_KEY],
            )
            .await
            .expect("register the class");
        client
            .execute(
                &format!(
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
                ),
                &[
                    &CLASS_KEY,
                    &DROPPED_RECORDS,
                    &(DROPPED_RECORDS + ACTIVE_RECORDS),
                ],
            )
            .await
            .expect("seed the catalog");
        client
            .batch_execute(&format!("ANALYZE {LEAF_CATALOG}"))
            .await
            .expect("analyze the catalog");
        let definition: Option<String> = client
            .query_one("SELECT pg_get_indexdef(to_regclass($1))", &[&ACTIVE_INDEX])
            .await
            .expect("read the index definition")
            .get(0);
        client
            .batch_execute(AUTO_EXPLAIN_SETTINGS)
            .await
            .expect("send the nested plans as notices");
        let limit = i32::try_from(MAX_PRUNE_CANDIDATES_PER_PASS + 1).expect("candidate limit");
        let candidates: i64 = client
            .query_one(
                "SELECT count(*) FROM horsies_prune_candidates($1)",
                &[&limit],
            )
            .await
            .expect("read the candidates")
            .get(0);
        client.batch_execute("ROLLBACK").await.expect("rollback");
        let messages = notices
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();

        // The index holds only records whose leaf is not dropped.
        assert!(
            definition
                .as_deref()
                .is_some_and(|text| text.ends_with(" WHERE (dropped_at IS NULL)")),
            "{definition:?}"
        );
        let scans: Vec<BTreeSet<String>> = logged_plans(&messages)
            .iter()
            .flat_map(catalog_scan_indexes)
            .collect();
        assert!(!scans.is_empty(), "{messages:?}");
        assert!(
            scans
                .iter()
                .all(|scan| scan.len() == 1 && scan.contains(ACTIVE_INDEX)),
            "{messages:?}"
        );
        // The seeded active records are expired, so the read returns them.
        assert!(
            candidates >= i64::from(ACTIVE_RECORDS),
            "{candidates} candidates"
        );
    }
}

//! Startup and periodic owner for coverage plus reader publication.

use chrono::{DateTime, Duration, Utc};

use crate::broker::core_rows::{one_row, query_in, scalar, StatementLog};

use crate::core::history::ddl::classes::DEFAULT_RETENTION_DAYS;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::HEARTBEAT_CLASS_KEY;
use crate::core::history::outcomes::LeafCreation;
use crate::core::history::periods::RetentionDays;
use crate::core::history::reads::publisher::publish_readers_in;
use crate::db::{
    CheckoutClass, CorePool, IsolationLevel, NotSentCause, PooledConnection, RollbackOutcome,
    StatementFailure, Transaction, TypedStatement,
};

pub use super::coverage_functions::CoverageRequest;
use super::coverage_functions::{
    create_heartbeat_leaf, create_history_leaf, probe_coverage, register_retention_classes,
    CoverageProbe, DamagedHeartbeatLeaf, DamagedHistoryLeaf, COVERAGE_PROBE, CREATE_HEARTBEAT_LEAF,
    CREATE_HISTORY_LEAF, REGISTER_CLASSES,
};

/// How far the history leaves reach (Python `HistoryCoverage`).
///
/// Serialized as the worker health payload form: `{"kind": "FOREVER"}` or
/// `{"kind": "UNTIL", "at": <RFC 3339 instant>}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HistoryCoverage {
    /// No history class has an end of coverage: only the forever class has
    /// leaves, and its leaf has an open upper bound.
    Forever,
    /// The first instant at which a history class has no leaf: the earliest,
    /// over the classes with daily leaves, of the upper bound of each
    /// class's newest leaf.
    Until { at: DateTime<Utc> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageEnsured {
    pub created_history_leaves: u64,
    pub created_heartbeat_leaves: u64,
    pub republished: bool,
    pub heartbeat_covered_now: bool,
    pub history_covered_through: HistoryCoverage,
    pub heartbeats_covered_through: DateTime<Utc>,
    pub absent_leaves: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageEnsureFailed {
    pub stage: &'static str,
    pub class_key: Option<String>,
    pub refusal: String,
    pub heartbeat_covered_now: bool,
    pub absent_leaves: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoverageOutcome {
    Ensured(CoverageEnsured),
    Failed(CoverageEnsureFailed),
}

impl CoverageOutcome {
    pub fn heartbeat_covered_now(&self) -> bool {
        match self {
            Self::Ensured(outcome) => outcome.heartbeat_covered_now,
            Self::Failed(outcome) => outcome.heartbeat_covered_now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupCoverageOutcome {
    Ready(CoverageOutcome),
    Refused(CoverageOutcome),
}

/// The default period and every declared period, each once, in day order.
pub(crate) fn registered_periods(declared_periods: &[RetentionDays]) -> Vec<RetentionDays> {
    let mut periods: Vec<RetentionDays> = declared_periods
        .iter()
        .copied()
        .chain(std::iter::once(DEFAULT_RETENTION_DAYS))
        .collect();
    periods.sort_unstable();
    periods.dedup();
    periods
}

/// Tries of one damaged leaf whose creation answers `Busy` (Python
/// `_LEAF_MAINTENANCE_ATTEMPTS`).
const LEAF_BUSY_ATTEMPTS: u32 = 3;
const LEAF_BUSY_BACKOFF_MS: u64 = 25;

/// The fewest connections of the session-work pool with which a gated pass
/// takes the coverage gate: the gate transaction holds one connection, and
/// the repair needs another (Python `coverage_gate_enabled`).
const GATED_PASS_MIN_CONNECTIONS: u32 = 2;

/// `pg_try_advisory_xact_lock` on the coverage gate (Python
/// `try_lock_coverage_for_transaction`, the same key).
static COVERAGE_GATE_TRY_LOCK: TypedStatement = TypedStatement::new(
    "coverage_gate_try_lock",
    "SELECT pg_try_advisory_xact_lock(hashtextextended('horsies:partition-coverage:v1', 1601))",
    &[],
);

async fn retry_busy_leaf(attempt: u32) {
    let jitter_ms = rand::random::<u64>() % LEAF_BUSY_BACKOFF_MS;
    let backoff_ms = LEAF_BUSY_BACKOFF_MS * u64::from(attempt) + jitter_ms;
    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
}

/// Checks out a session-work connection for maintenance work (coverage and
/// pruning) whose first statement is `first`. The reaper's maintenance runs
/// on the reserve ([`CheckoutClass::Reserved`]), as the reaper's recovery
/// statements do; on a pool with no reserve the class changes nothing.
///
/// Errors: the failed checkout as `first`'s `NotSent`.
pub(crate) async fn maintenance_connection(
    pool: &CorePool,
    first: &TypedStatement,
) -> Result<PooledConnection, HistoryError> {
    pool.checkout(CheckoutClass::Reserved)
        .await
        .map_err(|cause| {
            HistoryError::Statement(StatementFailure::NotSent {
                statement: first.name(),
                cause: NotSentCause::Checkout(cause),
            })
        })
}

/// The leaf that one leaf transaction serves.
#[derive(Clone, Copy)]
enum DamagedLeaf<'a> {
    History(&'a DamagedHistoryLeaf),
    Heartbeat(&'a DamagedHeartbeatLeaf),
}

/// One expected leaf in its own transaction: one call of
/// `horsies_create_history_leaf` or `horsies_create_heartbeat_leaf` (Python
/// `_maintain_history_leaf`, `_maintain_heartbeat_leaf`). `Busy` is retried
/// [`LEAF_BUSY_ATTEMPTS`] times; the transaction ends and the connection
/// goes back to the pool before each retry delay.
///
/// Errors: the checkout, `BEGIN`, the call or `COMMIT` failed
/// ([`HistoryError::Statement`]; nothing of this leaf committed, except
/// after `OutcomeUnknown` at `COMMIT`), or the row breaks the function's
/// contract.
async fn maintain_leaf(
    pool: &CorePool,
    log: StatementLog,
    leaf: DamagedLeaf<'_>,
) -> Result<LeafCreation, HistoryError> {
    let first = match leaf {
        DamagedLeaf::History(_) => &CREATE_HISTORY_LEAF,
        DamagedLeaf::Heartbeat(_) => &CREATE_HEARTBEAT_LEAF,
    };
    let mut attempt: u32 = 1;
    loop {
        let outcome = {
            let mut connection = maintenance_connection(pool, first).await?;
            let mut transaction = connection.begin(IsolationLevel::ServerDefault).await?;
            let outcome = match leaf {
                DamagedLeaf::History(leaf) => {
                    create_history_leaf(&mut transaction, log, leaf).await?
                }
                DamagedLeaf::Heartbeat(leaf) => {
                    create_heartbeat_leaf(&mut transaction, log, leaf).await?
                }
            };
            transaction.commit().await?;
            outcome
        };
        match outcome {
            LeafCreation::Busy { .. } if attempt < LEAF_BUSY_ATTEMPTS => {
                retry_busy_leaf(attempt).await;
                attempt += 1;
            }
            outcome => return Ok(outcome),
        }
    }
}

/// One probe statement on a connection of `pool`. Reads only.
async fn probe_in_pool(
    pool: &CorePool,
    log: StatementLog,
    request: &CoverageRequest,
) -> Result<CoverageProbe, HistoryError> {
    let mut connection = maintenance_connection(pool, &COVERAGE_PROBE).await?;
    probe_coverage(&mut connection, log, request).await
}

/// A healthy probe as the outcome of a pass that changed nothing.
fn ensured_from_probe(probe: CoverageProbe) -> CoverageOutcome {
    CoverageOutcome::Ensured(CoverageEnsured {
        created_history_leaves: 0,
        created_heartbeat_leaves: 0,
        republished: false,
        heartbeat_covered_now: probe.heartbeat_covered_now,
        history_covered_through: probe.history_covered_through,
        heartbeats_covered_through: probe.heartbeats_covered_through,
        absent_leaves: probe.absent_leaves,
    })
}

/// How a damaged coverage pass is serialized against other passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageSerialization {
    /// Worker startup: a damaged pass takes the coverage gate first, when
    /// the pool has at least two connections; a pass that finds the gate
    /// busy repairs nothing.
    Gated,
    /// The reaper's periodic pass: no gate. The per-leaf try-locks contain any
    /// overlap with a worker's startup pass.
    PerLeafLocks,
}

/// Whether a damaged pass takes the coverage gate. Pure.
///
/// A gated pass on a pool of one connection would hold that connection in
/// the gate transaction and wait for a second checkout until the pool
/// timeout, so it runs ungated, as Python (`coverage_gate_enabled`).
fn takes_coverage_gate(serialization: CoverageSerialization, pool_size: u32) -> bool {
    match serialization {
        CoverageSerialization::Gated => pool_size >= GATED_PASS_MIN_CONNECTIONS,
        CoverageSerialization::PerLeafLocks => false,
    }
}

/// Ensure coverage with one short transaction for each leaf, serialized by the
/// coverage gate (the startup rule).
///
/// `pool` must use a direct or session-capable PostgreSQL endpoint because
/// this function runs partition DDL. Errors: as [`run_coverage_pass`], and
/// [`HistoryError::Contract`] for a refused horizon.
pub(crate) async fn ensure_partition_coverage_in_pool(
    pool: &CorePool,
    log: StatementLog,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_periods: &[RetentionDays],
) -> Result<CoverageOutcome, HistoryError> {
    let request = CoverageRequest::new(
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_periods,
    )?;
    run_coverage_pass(pool, log, &request, CoverageSerialization::Gated).await
}

/// Ensure coverage for the reaper's periodic pass: the same steps as
/// `ensure_partition_coverage_in_pool` without the coverage gate, so a pass
/// during a worker's startup repair still repairs under the per-leaf locks.
pub(crate) async fn ensure_periodic_coverage_in_pool(
    pool: &CorePool,
    log: StatementLog,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_periods: &[RetentionDays],
) -> Result<CoverageOutcome, HistoryError> {
    let request = CoverageRequest::new(
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_periods,
    )?;
    run_coverage_pass(pool, log, &request, CoverageSerialization::PerLeafLocks).await
}

/// The coverage pass through the functions of
/// `0008_history_maintenance_functions.sql` (Python
/// `maintain_partition_coverage`), on the core pool `pool`. A healthy set
/// costs one probe statement (`horsies_coverage_probe`). A damaged set takes
/// the coverage gate when [`takes_coverage_gate`] says so, probes again under
/// it, and repairs: the registration when the probe finds it unhealthy, then
/// one transaction per damaged leaf. A created history leaf is published by
/// its own creation; the last step publishes the readers
/// (`horsies_publish_history_readers`) only when the probe still finds them
/// stale. Every checkout is reserved work ([`maintenance_connection`]).
///
/// A leaf's error or refusal is contained: its class stops, and the other
/// classes and the heartbeat leaves are still served; the outcome is
/// [`CoverageOutcome::Failed`] naming each failure.
///
/// Errors: [`HistoryError::Statement`] when a probe, the gate, the
/// registration or the final publication failed; [`HistoryError::RowDecode`]
/// and [`HistoryError::Contract`] for rows that break a function's contract.
/// The transactions that committed before the error stay committed (each
/// leaf commits alone); the gate is released.
pub(crate) async fn run_coverage_pass(
    pool: &CorePool,
    log: StatementLog,
    request: &CoverageRequest,
    serialization: CoverageSerialization,
) -> Result<CoverageOutcome, HistoryError> {
    let probe = probe_in_pool(pool, log, request).await?;
    if probe.is_healthy() {
        return Ok(ensured_from_probe(probe));
    }
    match takes_coverage_gate(serialization, pool.sizing().size()) {
        false => repair_partition_coverage(pool, log, request, probe).await,
        true => repair_under_coverage_gate(pool, log, request).await,
    }
}

/// Take the coverage gate, then probe and repair under it. A gate that
/// another pass holds repairs nothing: a probe that is healthy by then is
/// `Ensured`, else the pass is `Failed` at the stage `coverage_gate_busy`.
async fn repair_under_coverage_gate(
    pool: &CorePool,
    log: StatementLog,
    request: &CoverageRequest,
) -> Result<CoverageOutcome, HistoryError> {
    let mut gate_connection = maintenance_connection(pool, &COVERAGE_GATE_TRY_LOCK).await?;
    let mut gate = gate_connection.begin(IsolationLevel::ServerDefault).await?;
    let statement = &COVERAGE_GATE_TRY_LOCK;
    let rows = query_in(&mut gate, log, &statement.bind().finish()).await?;
    let acquired: bool = scalar(&one_row(rows, statement)?, statement)?;
    if !acquired {
        end_gate(gate).await;
        drop(gate_connection);
        let busy_probe = probe_in_pool(pool, log, request).await?;
        if busy_probe.is_healthy() {
            return Ok(ensured_from_probe(busy_probe));
        }
        return Ok(CoverageOutcome::Failed(CoverageEnsureFailed {
            stage: "coverage_gate_busy",
            class_key: busy_probe.first_damaged_class_key(),
            refusal: "another worker owns the partition coverage gate".to_owned(),
            heartbeat_covered_now: busy_probe.heartbeat_covered_now,
            absent_leaves: busy_probe.absent_leaves,
        }));
    }
    // Under the gate the probe runs again: another pass may have repaired the
    // set before the gate was granted.
    let outcome = match probe_in_pool(pool, log, request).await {
        Ok(locked_probe) if locked_probe.is_healthy() => Ok(ensured_from_probe(locked_probe)),
        Ok(locked_probe) => repair_partition_coverage(pool, log, request, locked_probe).await,
        Err(error) => Err(error),
    };
    end_gate(gate).await;
    outcome
}

/// Ends the gate transaction, which releases the gate. A failed `ROLLBACK`
/// discards the connection, and the server ends the transaction and its
/// lock when the socket closes, so either way the gate is released and the
/// outcome is discarded.
async fn end_gate(gate: Transaction<'_>) {
    let _released: RollbackOutcome = gate.rollback().await;
}

/// Repairs one damaged probe while the caller holds the gate, if any
/// (Python `_repair_partition_coverage`). A leaf that fails stops its class;
/// the other classes and the heartbeat leaves are still served.
async fn repair_partition_coverage(
    pool: &CorePool,
    log: StatementLog,
    request: &CoverageRequest,
    mut probe: CoverageProbe,
) -> Result<CoverageOutcome, HistoryError> {
    if !probe.registration_healthy {
        {
            let mut connection = maintenance_connection(pool, &REGISTER_CLASSES).await?;
            let mut transaction = connection.begin(IsolationLevel::ServerDefault).await?;
            register_retention_classes(&mut transaction, log, request).await?;
            transaction.commit().await?;
        }
        probe = probe_in_pool(pool, log, request).await?;
        if !probe.registration_healthy {
            return Ok(CoverageOutcome::Failed(CoverageEnsureFailed {
                stage: "verify_registration",
                class_key: None,
                refusal: "registered retention class shape is not conformant".to_owned(),
                heartbeat_covered_now: probe.heartbeat_covered_now,
                absent_leaves: probe.absent_leaves,
            }));
        }
    }

    let mut created_history = 0_u64;
    let mut created_heartbeats = 0_u64;
    let mut failures: Vec<String> = Vec::new();
    let mut failed_classes: Vec<String> = Vec::new();
    for leaf in &probe.damaged_history {
        if failed_classes.contains(&leaf.class_key) {
            continue;
        }
        match maintain_leaf(pool, log, DamagedLeaf::History(leaf)).await {
            Ok(LeafCreation::Created { .. }) => created_history += 1,
            Ok(
                LeafCreation::AlreadyConformant { .. }
                | LeafCreation::IndexRepaired { .. }
                | LeafCreation::CoveredByOpenEndedLeaf { .. },
            ) => {}
            Ok(refusal) => {
                failures.push(format!("{}: {refusal:?}", leaf.class_key));
                failed_classes.push(leaf.class_key.clone());
            }
            Err(error) => {
                failures.push(format!("{}: {error}", leaf.class_key));
                failed_classes.push(leaf.class_key.clone());
            }
        }
    }
    for leaf in &probe.damaged_heartbeats {
        match maintain_leaf(pool, log, DamagedLeaf::Heartbeat(leaf)).await {
            Ok(LeafCreation::Created { .. }) => created_heartbeats += 1,
            Ok(LeafCreation::AlreadyConformant { .. } | LeafCreation::IndexRepaired { .. }) => {}
            Ok(refusal) => {
                failures.push(format!("{HEARTBEAT_CLASS_KEY}: {refusal:?}"));
                break;
            }
            Err(error) => {
                failures.push(format!("{HEARTBEAT_CLASS_KEY}: {error}"));
                break;
            }
        }
    }

    let (final_probe, republished) = final_coverage_probe(pool, log, request).await?;
    if final_probe.is_healthy() {
        return Ok(CoverageOutcome::Ensured(CoverageEnsured {
            created_history_leaves: created_history,
            created_heartbeat_leaves: created_heartbeats,
            republished,
            heartbeat_covered_now: final_probe.heartbeat_covered_now,
            history_covered_through: final_probe.history_covered_through,
            heartbeats_covered_through: final_probe.heartbeats_covered_through,
            absent_leaves: final_probe.absent_leaves,
        }));
    }
    let refusal = match failures.is_empty() {
        true => final_probe.unresolved_detail(),
        false => failures.join("; "),
    };
    Ok(CoverageOutcome::Failed(CoverageEnsureFailed {
        stage: "ensure_leaf_coverage",
        class_key: final_probe.first_damaged_class_key(),
        refusal,
        heartbeat_covered_now: final_probe.heartbeat_covered_now,
        absent_leaves: final_probe.absent_leaves,
    }))
}

/// The last step of a repair, on one connection: a probe, and when it
/// finds the readers stale, one publication transaction
/// (`horsies_publish_history_readers`) and a second probe. Returns the last
/// probe and whether the readers were published.
async fn final_coverage_probe(
    pool: &CorePool,
    log: StatementLog,
    request: &CoverageRequest,
) -> Result<(CoverageProbe, bool), HistoryError> {
    let mut connection = maintenance_connection(pool, &COVERAGE_PROBE).await?;
    let probe = probe_coverage(&mut connection, log, request).await?;
    if probe.publication_healthy {
        return Ok((probe, false));
    }
    {
        let mut transaction = connection.begin(IsolationLevel::ServerDefault).await?;
        let _republished = publish_readers_in(&mut transaction, log).await?;
        transaction.commit().await?;
    }
    let probe = probe_coverage(&mut connection, log, request).await?;
    Ok((probe, true))
}

/// Whether a startup may go on after `outcome`: only when a heartbeat leaf
/// covers the present instant (Python `ensure_startup_coverage_in_database`).
/// Pure.
pub fn startup_verdict(outcome: CoverageOutcome) -> StartupCoverageOutcome {
    match outcome.heartbeat_covered_now() {
        true => StartupCoverageOutcome::Ready(outcome),
        false => StartupCoverageOutcome::Refused(outcome),
    }
}

/// Ensure startup coverage through a direct or session-capable core pool:
/// the gated pass, then [`startup_verdict`].
pub(crate) async fn ensure_startup_coverage_in_pool(
    pool: &CorePool,
    log: StatementLog,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_periods: &[RetentionDays],
) -> Result<StartupCoverageOutcome, HistoryError> {
    let outcome = ensure_partition_coverage_in_pool(
        pool,
        log,
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_periods,
    )
    .await?;
    Ok(startup_verdict(outcome))
}

#[cfg(test)]
mod gate_rule_tests {
    use super::*;

    /// Python `coverage_gate_enabled`: the startup pass takes the gate only
    /// with two connections or more; the periodic pass never takes it.
    #[test]
    fn the_gate_needs_two_connections_and_only_the_startup_pass_takes_it() {
        assert!(!takes_coverage_gate(CoverageSerialization::Gated, 1));
        assert!(takes_coverage_gate(CoverageSerialization::Gated, 2));
        assert!(takes_coverage_gate(CoverageSerialization::Gated, 10));
        assert!(!takes_coverage_gate(CoverageSerialization::PerLeafLocks, 1));
        assert!(!takes_coverage_gate(
            CoverageSerialization::PerLeafLocks,
            10
        ));
    }

    /// The startup verdict follows only `heartbeat_covered_now`, for an
    /// ensured and for a failed pass.
    #[test]
    fn the_startup_verdict_follows_the_present_heartbeat_coverage() {
        let failed = |covered: bool| {
            CoverageOutcome::Failed(CoverageEnsureFailed {
                stage: "ensure_leaf_coverage",
                class_key: Some("standard_30d".to_owned()),
                refusal: "1 class(es) failed".to_owned(),
                heartbeat_covered_now: covered,
                absent_leaves: Vec::new(),
            })
        };
        assert!(matches!(
            startup_verdict(failed(true)),
            StartupCoverageOutcome::Ready(CoverageOutcome::Failed(_))
        ));
        assert!(matches!(
            startup_verdict(failed(false)),
            StartupCoverageOutcome::Refused(CoverageOutcome::Failed(_))
        ));
    }
}

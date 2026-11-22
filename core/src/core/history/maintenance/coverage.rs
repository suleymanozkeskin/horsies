//! Startup and periodic owner for coverage plus reader publication.

use chrono::{DateTime, Utc};

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

/// The step at which a coverage pass stopped ([`CoverageEnsureFailed`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageFailureStage {
    /// A gated pass found the coverage gate held by another pass, and the
    /// probe after it found the set damaged. The pass repaired nothing.
    CoverageGateBusy,
    /// The registered retention classes were not conformant after the
    /// registration was written.
    VerifyRegistration,
    /// A leaf of the repair failed, or the final probe still found damage.
    EnsureLeafCoverage,
}

impl CoverageFailureStage {
    /// The stage as the outcome record of a client names it, for example
    /// `coverage_gate_busy`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CoverageGateBusy => "coverage_gate_busy",
            Self::VerifyRegistration => "verify_registration",
            Self::EnsureLeafCoverage => "ensure_leaf_coverage",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageEnsureFailed {
    pub stage: CoverageFailureStage,
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

/// The verdict of the startup coverage ([`startup_verdict`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupCoverageOutcome {
    /// A heartbeat leaf covers the present instant: the start goes on.
    Ready(CoverageOutcome),
    /// Another pass holds the coverage gate, and no heartbeat leaf covers
    /// the present instant. The condition can clear: the other pass creates
    /// the leaves and releases the gate, so the start runs the gated pass
    /// again after a backoff.
    GateBusy(CoverageEnsureFailed),
    /// No heartbeat leaf covers the present instant, for any other cause:
    /// the start is refused.
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
/// one transaction per damaged leaf, and one more round for the leaves of a
/// UTC hour or day that started during the repair
/// ([`LEAF_REPAIR_ROUNDS`]). A created history leaf is published by
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
            stage: CoverageFailureStage::CoverageGateBusy,
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

/// The rounds of leaf repair of one pass. A probe expects the leaves of its
/// own `statement_timestamp()`, so when the start of a UTC hour (or day)
/// falls between the probe that a round repairs and the final probe of the
/// round, the final probe expects a newest leaf that the round did not try
/// ([`CoverageProbe::has_damage_beyond`]). The second round repairs the
/// final probe of the first.
const LEAF_REPAIR_ROUNDS: u32 = 2;

/// The leaves that one round of repair created, and the contained failures
/// of the round (`<class key>: <cause>`, one per class that stopped).
#[derive(Debug, Default)]
struct LeafRepair {
    created_history: u64,
    created_heartbeats: u64,
    failures: Vec<String>,
}

/// The step after one round of leaf repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AfterRepairRound {
    /// The final probe of the round is healthy.
    Ensured,
    /// No leaf of the round failed, a round is left, and the final probe
    /// finds a damaged leaf that the repaired probe did not list.
    RepairAgain,
    /// A leaf of the round failed, no round is left, or the final probe
    /// finds damage that the round already tried.
    Failed,
}

/// The step after round `round` repaired `repaired`, made `repair` and
/// ended with `final_probe`. Pure.
fn after_repair_round(
    repaired: &CoverageProbe,
    repair: &LeafRepair,
    final_probe: &CoverageProbe,
    round: u32,
) -> AfterRepairRound {
    let repairable = repair.failures.is_empty()
        && round < LEAF_REPAIR_ROUNDS
        && final_probe.has_damage_beyond(repaired);
    match (final_probe.is_healthy(), repairable) {
        (true, _) => AfterRepairRound::Ensured,
        (false, true) => AfterRepairRound::RepairAgain,
        (false, false) => AfterRepairRound::Failed,
    }
}

/// Repairs one damaged probe while the caller holds the gate, if any
/// (Python `_repair_partition_coverage`): the registration, then up to
/// [`LEAF_REPAIR_ROUNDS`] rounds of leaf repair, each ended by the final
/// probe ([`final_coverage_probe`]). A leaf that fails stops its class; the
/// other classes and the heartbeat leaves are still served.
async fn repair_partition_coverage(
    pool: &CorePool,
    log: StatementLog,
    request: &CoverageRequest,
    probe: CoverageProbe,
) -> Result<CoverageOutcome, HistoryError> {
    let mut repaired = match repair_registration(pool, log, request, probe).await? {
        RegistrationStep::Healthy(probe) => probe,
        RegistrationStep::Refused(failed) => return Ok(CoverageOutcome::Failed(failed)),
    };
    let mut created_history = 0_u64;
    let mut created_heartbeats = 0_u64;
    let mut republished = false;
    let mut round: u32 = 1;
    loop {
        let repair = repair_damaged_leaves(pool, log, &repaired).await;
        let (final_probe, published) = final_coverage_probe(pool, log, request).await?;
        created_history += repair.created_history;
        created_heartbeats += repair.created_heartbeats;
        republished |= published;
        match after_repair_round(&repaired, &repair, &final_probe, round) {
            AfterRepairRound::Ensured => {
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
            AfterRepairRound::RepairAgain => {
                repaired = final_probe;
                round += 1;
            }
            AfterRepairRound::Failed => {
                return Ok(CoverageOutcome::Failed(failed_repair(
                    final_probe,
                    &repair.failures,
                )));
            }
        }
    }
}

/// The outcome of the registration step of a repair.
enum RegistrationStep {
    /// The probe to repair, with a healthy registration.
    Healthy(CoverageProbe),
    /// The registration is still not conformant after it was written.
    Refused(CoverageEnsureFailed),
}

/// The registration step of a repair: nothing when `probe` finds the
/// registration healthy; else one registration transaction
/// (`horsies_register_retention_classes`) and a new probe.
///
/// Errors: the checkout, the registration or the probe failed
/// ([`HistoryError::Statement`]); nothing of the registration committed,
/// except after `OutcomeUnknown` at `COMMIT`.
async fn repair_registration(
    pool: &CorePool,
    log: StatementLog,
    request: &CoverageRequest,
    probe: CoverageProbe,
) -> Result<RegistrationStep, HistoryError> {
    if probe.registration_healthy {
        return Ok(RegistrationStep::Healthy(probe));
    }
    {
        let mut connection = maintenance_connection(pool, &REGISTER_CLASSES).await?;
        let mut transaction = connection.begin(IsolationLevel::ServerDefault).await?;
        register_retention_classes(&mut transaction, log, request).await?;
        transaction.commit().await?;
    }
    let probe = probe_in_pool(pool, log, request).await?;
    if probe.registration_healthy {
        return Ok(RegistrationStep::Healthy(probe));
    }
    Ok(RegistrationStep::Refused(CoverageEnsureFailed {
        stage: CoverageFailureStage::VerifyRegistration,
        class_key: None,
        refusal: "registered retention class shape is not conformant".to_owned(),
        heartbeat_covered_now: probe.heartbeat_covered_now,
        absent_leaves: probe.absent_leaves,
    }))
}

/// One round of leaf repair: one transaction per damaged leaf of `probe`
/// ([`maintain_leaf`]), the history leaves first. An error or a refusal of
/// a history leaf stops its class; one of a heartbeat leaf stops the
/// heartbeat leaves. Each is recorded in [`LeafRepair::failures`], so the
/// round itself does not fail.
async fn repair_damaged_leaves(
    pool: &CorePool,
    log: StatementLog,
    probe: &CoverageProbe,
) -> LeafRepair {
    let mut repair = LeafRepair::default();
    let mut failed_classes: Vec<String> = Vec::new();
    for leaf in &probe.damaged_history {
        if failed_classes.contains(&leaf.class_key) {
            continue;
        }
        match maintain_leaf(pool, log, DamagedLeaf::History(leaf)).await {
            Ok(LeafCreation::Created { .. }) => repair.created_history += 1,
            Ok(
                LeafCreation::AlreadyConformant { .. }
                | LeafCreation::IndexRepaired { .. }
                | LeafCreation::CoveredByOpenEndedLeaf { .. },
            ) => {}
            Ok(refusal) => {
                repair
                    .failures
                    .push(format!("{}: {refusal:?}", leaf.class_key));
                failed_classes.push(leaf.class_key.clone());
            }
            Err(error) => {
                repair.failures.push(format!("{}: {error}", leaf.class_key));
                failed_classes.push(leaf.class_key.clone());
            }
        }
    }
    for leaf in &probe.damaged_heartbeats {
        match maintain_leaf(pool, log, DamagedLeaf::Heartbeat(leaf)).await {
            Ok(LeafCreation::Created { .. }) => repair.created_heartbeats += 1,
            Ok(LeafCreation::AlreadyConformant { .. } | LeafCreation::IndexRepaired { .. }) => {}
            Ok(refusal) => {
                repair
                    .failures
                    .push(format!("{HEARTBEAT_CLASS_KEY}: {refusal:?}"));
                break;
            }
            Err(error) => {
                repair
                    .failures
                    .push(format!("{HEARTBEAT_CLASS_KEY}: {error}"));
                break;
            }
        }
    }
    repair
}

/// The failed outcome of a repair whose last round ended with the unhealthy
/// `final_probe`: the round's failures, or what the probe still finds wrong
/// when no leaf failed. Pure.
fn failed_repair(final_probe: CoverageProbe, failures: &[String]) -> CoverageEnsureFailed {
    let refusal = match failures.is_empty() {
        true => final_probe.unresolved_detail(),
        false => failures.join("; "),
    };
    CoverageEnsureFailed {
        stage: CoverageFailureStage::EnsureLeafCoverage,
        class_key: final_probe.first_damaged_class_key(),
        refusal,
        heartbeat_covered_now: final_probe.heartbeat_covered_now,
        absent_leaves: final_probe.absent_leaves,
    }
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
/// With no such leaf, a pass that found the coverage gate busy
/// ([`CoverageFailureStage::CoverageGateBusy`]) is `GateBusy`, and every
/// other outcome is `Refused`. Pure.
pub fn startup_verdict(outcome: CoverageOutcome) -> StartupCoverageOutcome {
    match (outcome.heartbeat_covered_now(), outcome) {
        (true, outcome) => StartupCoverageOutcome::Ready(outcome),
        (
            false,
            CoverageOutcome::Failed(
                failed @ CoverageEnsureFailed {
                    stage: CoverageFailureStage::CoverageGateBusy,
                    ..
                },
            ),
        ) => StartupCoverageOutcome::GateBusy(failed),
        (false, outcome) => StartupCoverageOutcome::Refused(outcome),
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
                stage: CoverageFailureStage::EnsureLeafCoverage,
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

    /// A busy coverage gate with no present heartbeat leaf is `GateBusy`;
    /// with that leaf it is `Ready`. Every other failed stage with no present
    /// heartbeat leaf stays `Refused`.
    #[test]
    fn a_busy_gate_with_no_present_heartbeat_leaf_is_the_gate_busy_verdict() {
        let failed = |stage: CoverageFailureStage, covered: bool| {
            CoverageOutcome::Failed(CoverageEnsureFailed {
                stage,
                class_key: None,
                refusal: "a refusal".to_owned(),
                heartbeat_covered_now: covered,
                absent_leaves: Vec::new(),
            })
        };
        assert!(matches!(
            startup_verdict(failed(CoverageFailureStage::CoverageGateBusy, false)),
            StartupCoverageOutcome::GateBusy(CoverageEnsureFailed {
                stage: CoverageFailureStage::CoverageGateBusy,
                heartbeat_covered_now: false,
                ..
            })
        ));
        assert!(matches!(
            startup_verdict(failed(CoverageFailureStage::CoverageGateBusy, true)),
            StartupCoverageOutcome::Ready(CoverageOutcome::Failed(_))
        ));
        for stage in [
            CoverageFailureStage::VerifyRegistration,
            CoverageFailureStage::EnsureLeafCoverage,
        ] {
            assert!(
                matches!(
                    startup_verdict(failed(stage, false)),
                    StartupCoverageOutcome::Refused(CoverageOutcome::Failed(_))
                ),
                "{stage:?}"
            );
        }
    }
}

#[cfg(test)]
mod repair_round_tests {
    use chrono::TimeZone;

    use super::*;
    use crate::core::history::partitions::catalog::LeafLowerBound;

    /// A probe with healthy flags whose damaged leaves are `history` and
    /// `heartbeats`.
    fn probe(history: &[&str], heartbeats: &[&str]) -> CoverageProbe {
        let anchor = Utc
            .with_ymd_and_hms(2026, 11, 19, 4, 0, 0)
            .single()
            .expect("a valid instant");
        CoverageProbe {
            damaged_history: history
                .iter()
                .map(|leaf_name| DamagedHistoryLeaf {
                    class_key: "standard_30d".to_owned(),
                    leaf_name: (*leaf_name).to_owned(),
                    lower: LeafLowerBound::At(anchor),
                })
                .collect(),
            damaged_heartbeats: heartbeats
                .iter()
                .map(|leaf_name| DamagedHeartbeatLeaf {
                    leaf_name: (*leaf_name).to_owned(),
                    lower_anchor: anchor,
                })
                .collect(),
            registration_healthy: true,
            publication_healthy: true,
            heartbeat_covered_now: true,
            history_covered_through: HistoryCoverage::Forever,
            heartbeats_covered_through: anchor,
            absent_leaves: Vec::new(),
        }
    }

    fn failed_round() -> LeafRepair {
        LeafRepair {
            failures: vec!["standard_30d: Busy".to_owned()],
            ..LeafRepair::default()
        }
    }

    #[test]
    fn a_healthy_final_probe_is_ensured() {
        let repaired = probe(&[], &["hb_05"]);
        assert_eq!(
            after_repair_round(&repaired, &LeafRepair::default(), &probe(&[], &[]), 1),
            AfterRepairRound::Ensured,
        );
    }

    /// The start of an hour between the two probes: the final probe expects
    /// the leaf of the new hour, which the repaired probe did not list.
    #[test]
    fn a_heartbeat_leaf_that_the_repaired_probe_did_not_list_gets_another_round() {
        let repaired = probe(&[], &["hb_05"]);
        assert_eq!(
            after_repair_round(
                &repaired,
                &LeafRepair::default(),
                &probe(&[], &["hb_06"]),
                1,
            ),
            AfterRepairRound::RepairAgain,
        );
    }

    /// The start of a UTC day between the two probes, for a history leaf.
    #[test]
    fn a_history_leaf_that_the_repaired_probe_did_not_list_gets_another_round() {
        let repaired = probe(&["day_21"], &[]);
        assert_eq!(
            after_repair_round(
                &repaired,
                &LeafRepair::default(),
                &probe(&["day_22"], &[]),
                1,
            ),
            AfterRepairRound::RepairAgain,
        );
    }

    #[test]
    fn the_last_round_is_failed() {
        let repaired = probe(&[], &["hb_05"]);
        assert_eq!(
            after_repair_round(
                &repaired,
                &LeafRepair::default(),
                &probe(&[], &["hb_06"]),
                LEAF_REPAIR_ROUNDS,
            ),
            AfterRepairRound::Failed,
        );
    }

    #[test]
    fn a_round_with_a_failed_leaf_is_failed() {
        let repaired = probe(&[], &["hb_05"]);
        assert_eq!(
            after_repair_round(&repaired, &failed_round(), &probe(&[], &["hb_06"]), 1),
            AfterRepairRound::Failed,
        );
    }

    /// A leaf that the round tried and that is still damaged, or damage
    /// outside the leaves: another round would repeat the same work.
    #[test]
    fn damage_that_the_round_tried_is_failed() {
        let repaired = probe(&["day_21"], &["hb_05"]);
        assert_eq!(
            after_repair_round(
                &repaired,
                &LeafRepair::default(),
                &probe(&["day_21"], &["hb_05"]),
                1,
            ),
            AfterRepairRound::Failed,
        );
        let mut stale_readers = probe(&[], &[]);
        stale_readers.publication_healthy = false;
        assert_eq!(
            after_repair_round(&repaired, &LeafRepair::default(), &stale_readers, 1),
            AfterRepairRound::Failed,
        );
    }
}

#[cfg(test)]
mod repair_round_database_tests {
    use chrono::Duration;

    use super::*;
    use crate::broker::pools::core_pool_for_test_url;
    use crate::core::history::heartbeats::partitioning::hourly_leaf_name;
    use crate::test_support::sql::{self, TestPool};
    use crate::test_support::{migrated_pool, MIGRATED_HEARTBEAT_HOURS, MIGRATED_HISTORY_DAYS};

    /// The start of the current UTC hour of the database clock.
    async fn database_hour(pool: &TestPool) -> DateTime<Utc> {
        sql::query_scalar("SELECT date_trunc('hour', statement_timestamp(), 'UTC')")
            .fetch_one(pool)
            .await
            .expect("read the database hour")
    }

    /// Drops the leaf `leaf_name` and its catalog row (Python `_forget_leaf`).
    async fn forget_leaf(pool: &TestPool, leaf_name: &str) {
        sql::query(&format!("DROP TABLE {leaf_name}"))
            .execute(pool)
            .await
            .expect("drop the leaf");
        sql::query("DELETE FROM horsies_task_history_leaf_catalog WHERE leaf_name = $1")
            .bind(leaf_name)
            .execute(pool)
            .await
            .expect("forget the catalog row");
    }

    async fn relation_exists(pool: &TestPool, relation: &str) -> bool {
        sql::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(relation)
            .fetch_one(pool)
            .await
            .expect("read relation presence")
    }

    /// A repair whose probe was taken one hour before its final probe: the
    /// probe does not list the newest damaged heartbeat leaf of the final
    /// probe. With one round the pass is `Failed` at `ensure_leaf_coverage`
    /// with `unresolved coverage state: <that leaf>`; the second round
    /// creates the leaf and the pass is `Ensured`. The test holds when the
    /// hour of the database clock changes during it: the final probe then
    /// lists one more unlisted leaf, which the same second round creates.
    #[tokio::test]
    async fn a_leaf_of_an_hour_that_starts_during_the_repair_is_created_by_the_next_round() {
        let pool = migrated_pool().await;
        let hour = database_hour(&pool).await;
        for ahead in [1, 2] {
            let leaf_name = hourly_leaf_name(hour + Duration::hours(ahead)).expect("leaf name");
            forget_leaf(&pool, &leaf_name).await;
        }
        let core = core_pool_for_test_url(pool.url());
        let request = CoverageRequest::new(MIGRATED_HISTORY_DAYS, MIGRATED_HEARTBEAT_HOURS, &[])
            .expect("the coverage request of the test database");
        let mut repaired = probe_in_pool(&core, StatementLog::Off, &request)
            .await
            .expect("probe");
        let unlisted = repaired
            .damaged_heartbeats
            .pop()
            .expect("the probe lists the forgotten leaves");
        assert!(
            !repaired.damaged_heartbeats.is_empty(),
            "the repaired probe keeps a damaged heartbeat leaf: {repaired:?}"
        );

        let outcome = repair_partition_coverage(&core, StatementLog::Off, &request, repaired)
            .await
            .expect("repair");
        core.close();

        match outcome {
            CoverageOutcome::Ensured(ensured) => assert!(
                ensured.created_heartbeat_leaves >= 2,
                "both forgotten leaves are created: {ensured:?}"
            ),
            CoverageOutcome::Failed(failed) => panic!("expected an ensured pass, got {failed:?}"),
        }
        assert!(relation_exists(&pool, &unlisted.leaf_name).await);
    }
}

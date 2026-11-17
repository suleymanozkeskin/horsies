//! Startup and periodic owner for coverage plus reader publication.

use chrono::{DateTime, Duration, Timelike, Utc};
use sqlx::{Acquire, PgConnection, PgPool, Postgres, Transaction};

use crate::core::history::commands::EnsureLeafCoverage;
use crate::core::history::ddl::classes::{
    register_period_class, ClassRegistration, DEFAULT_RETENTION_DAYS, FOREVER_CLASS_KEY,
    PERIOD_CLASS_KIND,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::heartbeats::partitioning::{
    ensure_heartbeat_coverage, hourly_leaf_ref, register_heartbeat_class, EnsureHeartbeatCoverage,
    HeartbeatClassRegistration,
};
use crate::core::history::names::{HEARTBEAT_CLASS_KEY, RETENTION_CLASSES};
use crate::core::history::outcomes::LeafCreation;
use crate::core::history::partitions::catalog::database_now;
use crate::core::history::partitions::manager::ensure_leaf_coverage;
use crate::core::history::partitions::publication::{LoaderPublication, UnpublishedLoader};
use crate::core::history::periods::{HeartbeatHorizonHours, RetentionDays};

use super::coverage_functions::{
    create_heartbeat_leaf, create_history_leaf, probe_coverage, register_retention_classes,
    CoverageProbe, CoverageRequest, DamagedHeartbeatLeaf, DamagedHistoryLeaf,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageEnsured {
    pub created_history_leaves: u64,
    pub created_heartbeat_leaves: u64,
    pub republished: bool,
    pub heartbeat_covered_now: bool,
    pub history_covered_through: DateTime<Utc>,
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

pub async fn heartbeat_coverage_present(
    connection: &mut PgConnection,
) -> Result<bool, HistoryError> {
    let now = database_now(connection).await?;
    let lower = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(|| HistoryError::contract("database timestamp cannot be truncated"))?;
    let leaf = hourly_leaf_ref(lower)?;
    Ok(sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(leaf.leaf_name())
        .fetch_one(connection)
        .await?)
}

/// The `forever` class and every period class, by key.
async fn history_class_keys(connection: &mut PgConnection) -> Result<Vec<String>, HistoryError> {
    let sql = format!(
        "SELECT class_key FROM {RETENTION_CLASSES}
         WHERE class_kind = '{PERIOD_CLASS_KIND}' OR class_key = $1
         ORDER BY class_key"
    );
    Ok(sqlx::query_scalar(&sql)
        .bind(FOREVER_CLASS_KEY)
        .fetch_all(connection)
        .await?)
}

async fn register_partition_coverage(
    connection: &mut PgConnection,
    heartbeat_horizon_hours: u32,
    declared_periods: &[RetentionDays],
) -> Result<Option<CoverageEnsureFailed>, HistoryError> {
    let horizon = HeartbeatHorizonHours::new(heartbeat_horizon_hours)
        .map_err(|refused| HistoryError::contract(refused.to_string()))?;
    let heartbeat_registration = register_heartbeat_class(connection, horizon).await?;
    if matches!(
        heartbeat_registration,
        HeartbeatClassRegistration::ParentUnpartitioned
    ) {
        return Ok(Some(CoverageEnsureFailed {
            stage: "register_heartbeat_class",
            class_key: Some(HEARTBEAT_CLASS_KEY.to_owned()),
            refusal: format!("{heartbeat_registration:?}"),
            heartbeat_covered_now: heartbeat_coverage_present(connection).await?,
            absent_leaves: Vec::new(),
        }));
    }

    // A class key fixes its period, so registration cannot conflict: every
    // outcome is a registered class.
    for days in registered_periods(declared_periods) {
        match register_period_class(connection, days).await? {
            ClassRegistration::Registered { .. } | ClassRegistration::AlreadyRegistered { .. } => {}
        }
    }
    Ok(None)
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

pub async fn ensure_partition_coverage<P: LoaderPublication>(
    connection: &mut PgConnection,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_periods: &[RetentionDays],
    publisher: &P,
) -> Result<CoverageOutcome, HistoryError> {
    if let Some(failed) =
        register_partition_coverage(connection, heartbeat_horizon_hours, declared_periods).await?
    {
        return Ok(CoverageOutcome::Failed(failed));
    }

    let mut created_history = 0_u64;
    let mut failures = Vec::new();
    for (savepoint_number, class_key) in history_class_keys(connection)
        .await?
        .into_iter()
        .enumerate()
    {
        let savepoint = format!("horsies_history_coverage_{savepoint_number}");
        sqlx::query(&format!("SAVEPOINT {savepoint}"))
            .execute(&mut *connection)
            .await?;
        let command = EnsureLeafCoverage::new(&class_key, history_horizon_days)
            .map_err(|error| HistoryError::contract(error.to_string()))?;
        let class_result: Result<Vec<LeafCreation>, HistoryError> = async {
            let creations = ensure_leaf_coverage(connection, &command, &UnpublishedLoader).await?;
            if creations
                .iter()
                .any(|creation| matches!(creation, LeafCreation::Created { .. }))
            {
                publisher.republish(connection).await?;
            }
            Ok(creations)
        }
        .await;
        let creations = match class_result {
            Ok(creations) => {
                sqlx::query(&format!("RELEASE SAVEPOINT {savepoint}"))
                    .execute(&mut *connection)
                    .await?;
                creations
            }
            Err(error) => {
                sqlx::query(&format!("ROLLBACK TO SAVEPOINT {savepoint}"))
                    .execute(&mut *connection)
                    .await?;
                sqlx::query(&format!("RELEASE SAVEPOINT {savepoint}"))
                    .execute(&mut *connection)
                    .await?;
                failures.push(format!("{class_key}: {error}"));
                continue;
            }
        };
        created_history += creations
            .iter()
            .filter(|creation| matches!(creation, LeafCreation::Created { .. }))
            .count() as u64;
        let refusals: Vec<String> = creations
            .into_iter()
            .filter(|creation| {
                !matches!(
                    creation,
                    LeafCreation::Created { .. }
                        | LeafCreation::AlreadyConformant { .. }
                        | LeafCreation::IndexRepaired { .. }
                )
            })
            .map(|creation| format!("{creation:?}"))
            .collect();
        if !refusals.is_empty() {
            failures.push(format!("{class_key}: {}", refusals.join("; ")));
        }
    }

    let heartbeat_command = EnsureHeartbeatCoverage::new(heartbeat_horizon_hours)?;
    let heartbeat_creations = ensure_heartbeat_coverage(connection, &heartbeat_command).await?;
    let mut created_heartbeats = 0_u64;
    for creation in heartbeat_creations {
        match creation {
            LeafCreation::Created { .. } => created_heartbeats += 1,
            LeafCreation::AlreadyConformant { .. } | LeafCreation::IndexRepaired { .. } => {}
            refusal => {
                return Ok(CoverageOutcome::Failed(CoverageEnsureFailed {
                    stage: "ensure_heartbeat_coverage",
                    class_key: Some(HEARTBEAT_CLASS_KEY.to_owned()),
                    refusal: format!("{refusal:?}"),
                    heartbeat_covered_now: heartbeat_coverage_present(connection).await?,
                    absent_leaves: Vec::new(),
                }));
            }
        }
    }

    let mut republished = false;
    let mut absent_leaves = Vec::new();
    if created_history > 0 || publisher.needs_republication(connection).await? {
        let report = publisher.republish(connection).await?;
        republished = true;
        absent_leaves = report.absent_leaves;
    }
    if !failures.is_empty() {
        let first_class = failures[0]
            .split_once(':')
            .map(|(class, _)| class.to_owned());
        return Ok(CoverageOutcome::Failed(CoverageEnsureFailed {
            stage: "ensure_leaf_coverage",
            class_key: first_class,
            refusal: format!(
                "{} class(es) failed: {}",
                failures.len(),
                failures.join("; ")
            ),
            heartbeat_covered_now: heartbeat_coverage_present(connection).await?,
            absent_leaves,
        }));
    }
    let now = database_now(connection).await?;
    let day = now
        .with_hour(0)
        .and_then(|value| value.with_minute(0))
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(|| HistoryError::contract("database timestamp cannot be truncated"))?;
    let hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(|| HistoryError::contract("database timestamp cannot be truncated"))?;
    Ok(CoverageOutcome::Ensured(CoverageEnsured {
        created_history_leaves: created_history,
        created_heartbeat_leaves: created_heartbeats,
        republished,
        heartbeat_covered_now: heartbeat_coverage_present(connection).await?,
        history_covered_through: day + Duration::days(i64::from(history_horizon_days) + 1),
        heartbeats_covered_through: hour + Duration::hours(i64::from(heartbeat_horizon_hours) + 1),
        absent_leaves,
    }))
}

const LEAF_BUSY_ATTEMPTS: u32 = 3;
const LEAF_BUSY_BACKOFF_MS: u64 = 25;

async fn retry_busy_leaf(attempt: u32) {
    let jitter_ms = rand::random::<u64>() % LEAF_BUSY_BACKOFF_MS;
    let backoff_ms = LEAF_BUSY_BACKOFF_MS * u64::from(attempt) + jitter_ms;
    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
}

/// One expected history leaf in its own transaction: one call of
/// `horsies_create_history_leaf`. `Busy` is retried `LEAF_BUSY_ATTEMPTS`
/// times; the transaction ends before each retry delay.
async fn maintain_history_leaf(
    pool: &PgPool,
    leaf: &DamagedHistoryLeaf,
) -> Result<LeafCreation, HistoryError> {
    for attempt in 1..=LEAF_BUSY_ATTEMPTS {
        let mut transaction = pool.begin().await?;
        let outcome = create_history_leaf(transaction.as_mut(), leaf).await?;
        transaction.commit().await?;
        match outcome {
            LeafCreation::Busy { .. } if attempt < LEAF_BUSY_ATTEMPTS => {
                retry_busy_leaf(attempt).await;
            }
            outcome => return Ok(outcome),
        }
    }
    Err(HistoryError::contract(
        "history leaf retry loop ended without an outcome",
    ))
}

/// One expected heartbeat leaf in its own transaction: one call of
/// `horsies_create_heartbeat_leaf`, retried as [`maintain_history_leaf`].
async fn maintain_heartbeat_leaf(
    pool: &PgPool,
    leaf: &DamagedHeartbeatLeaf,
) -> Result<LeafCreation, HistoryError> {
    for attempt in 1..=LEAF_BUSY_ATTEMPTS {
        let mut transaction = pool.begin().await?;
        let outcome = create_heartbeat_leaf(transaction.as_mut(), leaf).await?;
        transaction.commit().await?;
        match outcome {
            LeafCreation::Busy { .. } if attempt < LEAF_BUSY_ATTEMPTS => {
                retry_busy_leaf(attempt).await;
            }
            outcome => return Ok(outcome),
        }
    }
    Err(HistoryError::contract(
        "heartbeat leaf retry loop ended without an outcome",
    ))
}

enum CoverageMaintenanceGate {
    Held(Transaction<'static, Postgres>),
    Busy,
    Ungated,
}

async fn acquire_coverage_maintenance_gate(
    pool: &PgPool,
) -> Result<CoverageMaintenanceGate, HistoryError> {
    if pool.options().get_max_connections() < 2 {
        return Ok(CoverageMaintenanceGate::Ungated);
    }
    let mut transaction = pool.begin().await?;
    let acquired: bool = sqlx::query_scalar(
        "SELECT pg_try_advisory_xact_lock(
             hashtextextended('horsies:partition-coverage:v1', 1601)
         )",
    )
    .fetch_one(transaction.as_mut())
    .await?;
    match acquired {
        true => Ok(CoverageMaintenanceGate::Held(transaction)),
        false => Ok(CoverageMaintenanceGate::Busy),
    }
}

async fn release_coverage_maintenance_gate(
    gate: CoverageMaintenanceGate,
) -> Result<(), HistoryError> {
    match gate {
        CoverageMaintenanceGate::Held(transaction) => {
            transaction.commit().await?;
        }
        CoverageMaintenanceGate::Busy | CoverageMaintenanceGate::Ungated => {}
    }
    Ok(())
}

/// One probe statement on a connection of `pool`. Reads only.
async fn probe_in_pool(
    pool: &PgPool,
    request: &CoverageRequest,
) -> Result<CoverageProbe, HistoryError> {
    let mut connection = pool.acquire().await?;
    probe_coverage(&mut connection, request).await
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
    /// Worker startup: a damaged pass takes the coverage gate first; a pass
    /// that finds it busy repairs nothing.
    Gated,
    /// The reaper's periodic pass: no gate. The per-leaf try-locks contain any
    /// overlap with a worker's startup pass.
    PerLeafLocks,
}

/// Ensure coverage with one short transaction for each leaf, serialized by the
/// coverage gate (the startup rule).
///
/// `pool` must use a direct or session-capable PostgreSQL endpoint because
/// this function runs partition DDL.
pub async fn ensure_partition_coverage_in_pool<P: LoaderPublication>(
    pool: &PgPool,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_periods: &[RetentionDays],
    publisher: &P,
) -> Result<CoverageOutcome, HistoryError> {
    ensure_coverage_in_pool(
        pool,
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_periods,
        publisher,
        CoverageSerialization::Gated,
    )
    .await
}

/// Ensure coverage for the reaper's periodic pass: the same steps as
/// `ensure_partition_coverage_in_pool` without the coverage gate, so a pass
/// during a worker's startup repair still repairs under the per-leaf locks.
pub async fn ensure_periodic_coverage_in_pool<P: LoaderPublication>(
    pool: &PgPool,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_periods: &[RetentionDays],
    publisher: &P,
) -> Result<CoverageOutcome, HistoryError> {
    ensure_coverage_in_pool(
        pool,
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_periods,
        publisher,
        CoverageSerialization::PerLeafLocks,
    )
    .await
}

/// The coverage pass through the functions of
/// `0008_history_maintenance_functions.sql` (Python
/// `maintain_partition_coverage`). A healthy set costs one probe statement
/// (`horsies_coverage_probe`). A damaged set takes the coverage gate
/// (`Gated`), probes again under it, and repairs: the registration when the
/// probe finds it unhealthy, then one transaction per damaged leaf. A
/// created history leaf is published by its own creation; the last step
/// publishes through `publisher` only when the probe still finds the readers
/// stale.
async fn ensure_coverage_in_pool<P: LoaderPublication>(
    pool: &PgPool,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_periods: &[RetentionDays],
    publisher: &P,
    serialization: CoverageSerialization,
) -> Result<CoverageOutcome, HistoryError> {
    let request = CoverageRequest::new(
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_periods,
    )?;
    let probe = probe_in_pool(pool, &request).await?;
    if probe.is_healthy() {
        return Ok(ensured_from_probe(probe));
    }
    let gate = match serialization {
        CoverageSerialization::Gated => acquire_coverage_maintenance_gate(pool).await?,
        CoverageSerialization::PerLeafLocks => CoverageMaintenanceGate::Ungated,
    };
    // The gate's kind is read before each await: no borrow of the gate
    // transaction is held across one.
    if matches!(gate, CoverageMaintenanceGate::Busy) {
        let busy_probe = probe_in_pool(pool, &request).await?;
        if busy_probe.is_healthy() {
            return Ok(ensured_from_probe(busy_probe));
        }
        return Ok(CoverageOutcome::Failed(CoverageEnsureFailed {
            stage: "coverage_gate_busy",
            class_key: busy_probe.first_damaged_class_key(),
            refusal: "partition coverage maintenance is active on another worker".to_owned(),
            heartbeat_covered_now: busy_probe.heartbeat_covered_now,
            absent_leaves: busy_probe.absent_leaves,
        }));
    }
    // Under a held gate the probe runs again: another pass may have repaired
    // the set before the gate was granted.
    let locked_probe = match matches!(gate, CoverageMaintenanceGate::Held(_)) {
        true => probe_in_pool(pool, &request).await?,
        false => probe,
    };
    if locked_probe.is_healthy() {
        release_coverage_maintenance_gate(gate).await?;
        return Ok(ensured_from_probe(locked_probe));
    }
    let outcome = repair_partition_coverage(pool, &request, locked_probe, publisher).await;
    release_coverage_maintenance_gate(gate).await?;
    outcome
}

/// Repairs one damaged probe while the caller holds the gate, if any
/// (Python `_repair_partition_coverage`). A leaf that fails stops its class;
/// the other classes and the heartbeat leaves are still served.
async fn repair_partition_coverage<P: LoaderPublication>(
    pool: &PgPool,
    request: &CoverageRequest,
    mut probe: CoverageProbe,
    publisher: &P,
) -> Result<CoverageOutcome, HistoryError> {
    if !probe.registration_healthy {
        let mut transaction = pool.begin().await?;
        register_retention_classes(transaction.as_mut(), request).await?;
        transaction.commit().await?;
        probe = probe_in_pool(pool, request).await?;
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
        match maintain_history_leaf(pool, leaf).await {
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
        match maintain_heartbeat_leaf(pool, leaf).await {
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

    let mut connection = pool.acquire().await?;
    let mut final_probe = probe_coverage(&mut connection, request).await?;
    let mut republished = false;
    if !final_probe.publication_healthy {
        let mut transaction = connection.begin().await?;
        let _report = publisher.republish(transaction.as_mut()).await?;
        transaction.commit().await?;
        republished = true;
        final_probe = probe_coverage(&mut connection, request).await?;
    }
    drop(connection);

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

pub async fn ensure_startup_coverage<P: LoaderPublication>(
    connection: &mut PgConnection,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_periods: &[RetentionDays],
    publisher: &P,
) -> Result<StartupCoverageOutcome, HistoryError> {
    let outcome = ensure_partition_coverage(
        connection,
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_periods,
        publisher,
    )
    .await?;
    if outcome.heartbeat_covered_now() {
        Ok(StartupCoverageOutcome::Ready(outcome))
    } else {
        Ok(StartupCoverageOutcome::Refused(outcome))
    }
}

/// Ensure startup coverage through a direct or session-capable pool.
pub async fn ensure_startup_coverage_in_pool<P: LoaderPublication>(
    pool: &PgPool,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_periods: &[RetentionDays],
    publisher: &P,
) -> Result<StartupCoverageOutcome, HistoryError> {
    let outcome = ensure_partition_coverage_in_pool(
        pool,
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_periods,
        publisher,
    )
    .await?;
    if outcome.heartbeat_covered_now() {
        Ok(StartupCoverageOutcome::Ready(outcome))
    } else {
        Ok(StartupCoverageOutcome::Refused(outcome))
    }
}

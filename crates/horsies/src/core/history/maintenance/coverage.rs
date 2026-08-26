//! Startup and periodic owner for coverage plus reader publication.

use chrono::{DateTime, Duration, Timelike, Utc};
use sqlx::{Acquire, PgConnection, PgPool, Postgres, Transaction};

use crate::core::history::commands::{CreateDailyHistoryLeaf, EnsureLeafCoverage};
use crate::core::history::ddl::classes::{
    register_finite_retention_class, ClassRegistration, DEFAULT_RETENTION_CLASS_KEY,
    DEFAULT_RETENTION_DURATION_DAYS, FOREVER_CLASS_KEY,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::heartbeats::partitioning::{
    create_hourly_heartbeat_leaf, ensure_heartbeat_coverage, hourly_leaf_ref,
    register_heartbeat_class, CreateHourlyHeartbeatLeaf, EnsureHeartbeatCoverage,
    HeartbeatClassRegistration,
};
use crate::core::history::names::{HEARTBEAT_CLASS_KEY, RETENTION_CLASSES};
use crate::core::history::outcomes::LeafCreation;
use crate::core::history::partitions::catalog::database_now;
use crate::core::history::partitions::manager::{create_daily_leaf, ensure_leaf_coverage};
use crate::core::history::partitions::publication::{LoaderPublication, UnpublishedLoader};

use super::coverage_probe::{probe_partition_coverage, CoverageLeafRepair, CoverageProbe};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredRetentionClass {
    pub class_key: String,
    pub duration: Duration,
}

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

async fn history_class_keys(connection: &mut PgConnection) -> Result<Vec<String>, HistoryError> {
    let sql = format!(
        "SELECT class_key FROM {RETENTION_CLASSES}
         WHERE (duration IS NOT NULL OR class_key = $1) AND class_key <> $2
         ORDER BY class_key"
    );
    Ok(sqlx::query_scalar(&sql)
        .bind(FOREVER_CLASS_KEY)
        .bind(HEARTBEAT_CLASS_KEY)
        .fetch_all(connection)
        .await?)
}

async fn register_partition_coverage(
    connection: &mut PgConnection,
    heartbeat_horizon_hours: u32,
    declared_classes: &[DeclaredRetentionClass],
) -> Result<Option<CoverageEnsureFailed>, HistoryError> {
    let heartbeat_registration = register_heartbeat_class(
        connection,
        Duration::hours(i64::from(heartbeat_horizon_hours)),
    )
    .await?;
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

    let default_registration = register_finite_retention_class(
        connection,
        DEFAULT_RETENTION_CLASS_KEY,
        Duration::days(DEFAULT_RETENTION_DURATION_DAYS),
    )
    .await?;
    if !matches!(
        default_registration,
        ClassRegistration::Registered { .. } | ClassRegistration::AlreadyRegistered { .. }
    ) {
        return Ok(Some(CoverageEnsureFailed {
            stage: "register_default_class",
            class_key: Some(DEFAULT_RETENTION_CLASS_KEY.to_owned()),
            refusal: format!("{default_registration:?}"),
            heartbeat_covered_now: heartbeat_coverage_present(connection).await?,
            absent_leaves: Vec::new(),
        }));
    }
    for declared in declared_classes {
        let registration =
            register_finite_retention_class(connection, &declared.class_key, declared.duration)
                .await?;
        if !matches!(
            registration,
            ClassRegistration::Registered { .. } | ClassRegistration::AlreadyRegistered { .. }
        ) {
            return Ok(Some(CoverageEnsureFailed {
                stage: "register_declared_class",
                class_key: Some(declared.class_key.clone()),
                refusal: format!("{registration:?}"),
                heartbeat_covered_now: heartbeat_coverage_present(connection).await?,
                absent_leaves: Vec::new(),
            }));
        }
    }
    Ok(None)
}

pub async fn ensure_partition_coverage<P: LoaderPublication>(
    connection: &mut PgConnection,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_classes: &[DeclaredRetentionClass],
    publisher: &P,
) -> Result<CoverageOutcome, HistoryError> {
    if let Some(failed) =
        register_partition_coverage(connection, heartbeat_horizon_hours, declared_classes).await?
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
        let class_result = ensure_leaf_coverage(connection, &command, publisher).await;
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

async fn create_daily_leaf_in_own_transaction(
    pool: &PgPool,
    command: &CreateDailyHistoryLeaf,
) -> Result<LeafCreation, HistoryError> {
    for attempt in 1..=LEAF_BUSY_ATTEMPTS {
        let mut transaction = pool.begin().await?;
        let outcome = create_daily_leaf(transaction.as_mut(), command, &UnpublishedLoader).await?;
        transaction.commit().await?;
        match outcome {
            LeafCreation::Busy { .. } if attempt < LEAF_BUSY_ATTEMPTS => {
                retry_busy_leaf(attempt).await;
            }
            outcome => return Ok(outcome),
        }
    }
    Err(HistoryError::contract(
        "daily leaf retry loop ended without an outcome",
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

fn first_probe_class_key(probe: &CoverageProbe) -> Option<String> {
    probe
        .class_faults
        .first()
        .map(|fault| fault.class_key.clone())
        .or_else(|| {
            probe.leaf_repairs.first().map(|repair| match repair {
                CoverageLeafRepair::History(command) => command.leaf().class_key().to_owned(),
                CoverageLeafRepair::Heartbeat(command) => command.leaf().class_key().to_owned(),
            })
        })
}

fn failed_probe(
    stage: &'static str,
    probe: &CoverageProbe,
    mut details: Vec<String>,
    absent_leaves: Vec<String>,
) -> CoverageOutcome {
    details.extend(
        probe
            .class_faults
            .iter()
            .map(|fault| format!("{}: {}", fault.class_key, fault.detail)),
    );
    details.extend(probe.leaf_repairs.iter().map(|repair| match repair {
        CoverageLeafRepair::History(command) => format!(
            "{}: required leaf {:?} remains nonconformant",
            command.leaf().class_key(),
            command.leaf().leaf_name()
        ),
        CoverageLeafRepair::Heartbeat(command) => format!(
            "{}: required leaf {:?} remains nonconformant",
            command.leaf().class_key(),
            command.leaf().leaf_name()
        ),
    }));
    CoverageOutcome::Failed(CoverageEnsureFailed {
        stage,
        class_key: first_probe_class_key(probe),
        refusal: details.join("; "),
        heartbeat_covered_now: probe.heartbeat_covered_now,
        absent_leaves,
    })
}

async fn publish_coverage_if_needed<P: LoaderPublication>(
    pool: &PgPool,
    publisher: &P,
    force: bool,
) -> Result<(bool, Vec<String>), HistoryError> {
    let mut connection = pool.acquire().await?;
    if !force && !publisher.needs_republication(&mut connection).await? {
        return Ok((false, Vec::new()));
    }
    let mut transaction = connection.begin().await?;
    let report = publisher.republish(transaction.as_mut()).await?;
    transaction.commit().await?;
    Ok((true, report.absent_leaves))
}

fn ensured_probe(
    probe: &CoverageProbe,
    created_history_leaves: u64,
    created_heartbeat_leaves: u64,
    republished: bool,
    absent_leaves: Vec<String>,
) -> CoverageOutcome {
    CoverageOutcome::Ensured(CoverageEnsured {
        created_history_leaves,
        created_heartbeat_leaves,
        republished,
        heartbeat_covered_now: probe.heartbeat_covered_now,
        history_covered_through: probe.history_covered_through,
        heartbeats_covered_through: probe.heartbeats_covered_through,
        absent_leaves,
    })
}

async fn create_heartbeat_leaf_in_own_transaction(
    pool: &PgPool,
    command: &CreateHourlyHeartbeatLeaf,
) -> Result<LeafCreation, HistoryError> {
    for attempt in 1..=LEAF_BUSY_ATTEMPTS {
        let mut transaction = pool.begin().await?;
        let outcome = create_hourly_heartbeat_leaf(transaction.as_mut(), command).await?;
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

/// Ensure coverage with one short transaction for each leaf.
///
/// `pool` must use a direct or session-capable PostgreSQL endpoint because
/// this function runs partition DDL.
pub async fn ensure_partition_coverage_in_pool<P: LoaderPublication>(
    pool: &PgPool,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_classes: &[DeclaredRetentionClass],
    publisher: &P,
) -> Result<CoverageOutcome, HistoryError> {
    EnsureLeafCoverage::new(FOREVER_CLASS_KEY, history_horizon_days)
        .map_err(|error| HistoryError::contract(error.to_string()))?;
    EnsureHeartbeatCoverage::new(heartbeat_horizon_hours)?;
    let mut connection = pool.acquire().await?;
    let initial_probe = probe_partition_coverage(
        &mut connection,
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_classes,
    )
    .await?;
    if initial_probe.is_conformant() {
        let needs_publication = publisher.needs_republication(&mut connection).await?;
        drop(connection);
        if !needs_publication {
            return Ok(ensured_probe(&initial_probe, 0, 0, false, Vec::new()));
        }
        let (republished, absent_leaves) =
            publish_coverage_if_needed(pool, publisher, true).await?;
        return Ok(ensured_probe(
            &initial_probe,
            0,
            0,
            republished,
            absent_leaves,
        ));
    }
    drop(connection);

    let gate = acquire_coverage_maintenance_gate(pool).await?;
    if matches!(gate, CoverageMaintenanceGate::Busy) {
        let mut connection = pool.acquire().await?;
        let current = probe_partition_coverage(
            &mut connection,
            history_horizon_days,
            heartbeat_horizon_hours,
            declared_classes,
        )
        .await?;
        if current.is_conformant() {
            let needs_publication = publisher.needs_republication(&mut connection).await?;
            if !needs_publication {
                return Ok(ensured_probe(&current, 0, 0, false, Vec::new()));
            }
        }
        return Ok(failed_probe(
            "coverage_gate_busy",
            &current,
            vec!["partition coverage maintenance is active on another worker".to_owned()],
            Vec::new(),
        ));
    }

    let mut connection = pool.acquire().await?;
    let mut repair_probe = probe_partition_coverage(
        &mut connection,
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_classes,
    )
    .await?;
    drop(connection);
    if repair_probe.is_conformant() {
        release_coverage_maintenance_gate(gate).await?;
        let (republished, absent_leaves) =
            publish_coverage_if_needed(pool, publisher, false).await?;
        return Ok(ensured_probe(
            &repair_probe,
            0,
            0,
            republished,
            absent_leaves,
        ));
    }

    if !repair_probe.class_faults.is_empty() {
        let mut registration = pool.begin().await?;
        let registration_failure = register_partition_coverage(
            registration.as_mut(),
            heartbeat_horizon_hours,
            declared_classes,
        )
        .await?;
        registration.commit().await?;
        if let Some(failed) = registration_failure {
            release_coverage_maintenance_gate(gate).await?;
            return Ok(CoverageOutcome::Failed(failed));
        }
        let mut connection = pool.acquire().await?;
        repair_probe = probe_partition_coverage(
            &mut connection,
            history_horizon_days,
            heartbeat_horizon_hours,
            declared_classes,
        )
        .await?;
    }

    let mut created_history = 0_u64;
    let mut created_heartbeats = 0_u64;
    let mut failures = Vec::new();
    for repair in repair_probe.leaf_repairs {
        match repair {
            CoverageLeafRepair::History(create) => {
                let class_key = create.leaf().class_key().to_owned();
                let outcome = match create_daily_leaf_in_own_transaction(pool, &create).await {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        failures.push(format!("{class_key}: {error}"));
                        continue;
                    }
                };
                match outcome {
                    LeafCreation::Created { .. } => created_history += 1,
                    LeafCreation::AlreadyConformant { .. } | LeafCreation::IndexRepaired { .. } => {
                    }
                    refusal => failures.push(format!("{class_key}: {refusal:?}")),
                }
            }
            CoverageLeafRepair::Heartbeat(create) => {
                match create_heartbeat_leaf_in_own_transaction(pool, &create).await {
                    Ok(LeafCreation::Created { .. }) => created_heartbeats += 1,
                    Ok(
                        LeafCreation::AlreadyConformant { .. } | LeafCreation::IndexRepaired { .. },
                    ) => {}
                    Ok(refusal) => {
                        failures.push(format!("{}: {refusal:?}", create.leaf().class_key()));
                    }
                    Err(error) => failures.push(format!("{}: {error}", create.leaf().class_key())),
                }
            }
        }
    }

    let mut connection = pool.acquire().await?;
    let final_probe = probe_partition_coverage(
        &mut connection,
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_classes,
    )
    .await?;
    drop(connection);
    let (republished, absent_leaves) =
        publish_coverage_if_needed(pool, publisher, created_history > 0).await?;
    release_coverage_maintenance_gate(gate).await?;
    if final_probe.is_conformant() && failures.is_empty() {
        return Ok(ensured_probe(
            &final_probe,
            created_history,
            created_heartbeats,
            republished,
            absent_leaves,
        ));
    }
    Ok(failed_probe(
        "ensure_partition_coverage",
        &final_probe,
        failures,
        absent_leaves,
    ))
}

pub async fn ensure_startup_coverage<P: LoaderPublication>(
    connection: &mut PgConnection,
    history_horizon_days: u32,
    heartbeat_horizon_hours: u32,
    declared_classes: &[DeclaredRetentionClass],
    publisher: &P,
) -> Result<StartupCoverageOutcome, HistoryError> {
    let outcome = ensure_partition_coverage(
        connection,
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_classes,
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
    declared_classes: &[DeclaredRetentionClass],
    publisher: &P,
) -> Result<StartupCoverageOutcome, HistoryError> {
    let outcome = ensure_partition_coverage_in_pool(
        pool,
        history_horizon_days,
        heartbeat_horizon_hours,
        declared_classes,
        publisher,
    )
    .await?;
    if outcome.heartbeat_covered_now() {
        Ok(StartupCoverageOutcome::Ready(outcome))
    } else {
        Ok(StartupCoverageOutcome::Refused(outcome))
    }
}

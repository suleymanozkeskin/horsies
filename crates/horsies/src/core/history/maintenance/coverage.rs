//! Startup and periodic owner for coverage plus reader publication.

use chrono::{DateTime, Duration, Timelike, Utc};
use sqlx::{PgConnection, PgPool};

use crate::core::history::commands::{CreateDailyHistoryLeaf, EnsureLeafCoverage};
use crate::core::history::ddl::classes::{
    register_finite_retention_class, ClassRegistration, DEFAULT_RETENTION_CLASS_KEY,
    DEFAULT_RETENTION_DURATION_DAYS, FOREVER_CLASS_KEY,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::heartbeats::partitioning::{
    create_hourly_heartbeat_leaf, ensure_heartbeat_coverage, hourly_leaf_ref,
    plan_heartbeat_coverage, register_heartbeat_class, CreateHourlyHeartbeatLeaf,
    EnsureHeartbeatCoverage, HeartbeatClassRegistration,
};
use crate::core::history::names::{HEARTBEAT_CLASS_KEY, RETENTION_CLASSES};
use crate::core::history::outcomes::LeafCreation;
use crate::core::history::partitions::catalog::database_now;
use crate::core::history::partitions::manager::{
    create_daily_leaf, ensure_leaf_coverage, plan_daily_leaf_coverage, DailyCoveragePlan,
};
use crate::core::history::partitions::publication::LoaderPublication;

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

async fn create_daily_leaf_in_own_transaction<P: LoaderPublication>(
    pool: &PgPool,
    command: &CreateDailyHistoryLeaf,
    publisher: &P,
) -> Result<LeafCreation, HistoryError> {
    for attempt in 1..=LEAF_BUSY_ATTEMPTS {
        let mut transaction = pool.begin().await?;
        let outcome = create_daily_leaf(transaction.as_mut(), command, publisher).await?;
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
    let mut registration = pool.begin().await?;
    let registration_failure = register_partition_coverage(
        registration.as_mut(),
        heartbeat_horizon_hours,
        declared_classes,
    )
    .await?;
    registration.commit().await?;
    if let Some(failed) = registration_failure {
        return Ok(CoverageOutcome::Failed(failed));
    }

    let class_keys = {
        let mut connection = pool.acquire().await?;
        history_class_keys(&mut connection).await?
    };
    let mut created_history = 0_u64;
    let mut failures = Vec::new();
    for class_key in class_keys {
        let command = EnsureLeafCoverage::new(&class_key, history_horizon_days)
            .map_err(|error| HistoryError::contract(error.to_string()))?;
        let plan = {
            let mut connection = pool.acquire().await?;
            plan_daily_leaf_coverage(&mut connection, &command).await?
        };
        let commands = match plan {
            DailyCoveragePlan::Leaves(commands) => commands,
            DailyCoveragePlan::Refused(refusal) => {
                failures.push(format!("{class_key}: {refusal:?}"));
                continue;
            }
        };
        for create in commands {
            let outcome = match create_daily_leaf_in_own_transaction(pool, &create, publisher).await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    failures.push(format!("{class_key}: {error}"));
                    break;
                }
            };
            match outcome {
                LeafCreation::Created { .. } => created_history += 1,
                LeafCreation::AlreadyConformant { .. } | LeafCreation::IndexRepaired { .. } => {}
                refusal => {
                    failures.push(format!("{class_key}: {refusal:?}"));
                    break;
                }
            }
        }
    }

    let heartbeat_command = EnsureHeartbeatCoverage::new(heartbeat_horizon_hours)?;
    let heartbeat_plan = {
        let mut connection = pool.acquire().await?;
        plan_heartbeat_coverage(&mut connection, &heartbeat_command).await?
    };
    let mut created_heartbeats = 0_u64;
    for create in heartbeat_plan {
        match create_heartbeat_leaf_in_own_transaction(pool, &create).await? {
            LeafCreation::Created { .. } => created_heartbeats += 1,
            LeafCreation::AlreadyConformant { .. } | LeafCreation::IndexRepaired { .. } => {}
            refusal => {
                let mut connection = pool.acquire().await?;
                return Ok(CoverageOutcome::Failed(CoverageEnsureFailed {
                    stage: "ensure_heartbeat_coverage",
                    class_key: Some(HEARTBEAT_CLASS_KEY.to_owned()),
                    refusal: format!("{refusal:?}"),
                    heartbeat_covered_now: heartbeat_coverage_present(&mut connection).await?,
                    absent_leaves: Vec::new(),
                }));
            }
        }
    }

    let mut finalization = pool.begin().await?;
    let mut republished = false;
    let mut absent_leaves = Vec::new();
    if created_history > 0 || publisher.needs_republication(finalization.as_mut()).await? {
        let report = publisher.republish(finalization.as_mut()).await?;
        republished = true;
        absent_leaves = report.absent_leaves;
    }
    let heartbeat_covered_now = heartbeat_coverage_present(finalization.as_mut()).await?;
    let now = database_now(finalization.as_mut()).await?;
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
    finalization.commit().await?;

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
            heartbeat_covered_now,
            absent_leaves,
        }));
    }
    Ok(CoverageOutcome::Ensured(CoverageEnsured {
        created_history_leaves: created_history,
        created_heartbeat_leaves: created_heartbeats,
        republished,
        heartbeat_covered_now,
        history_covered_through: day + Duration::days(i64::from(history_horizon_days) + 1),
        heartbeats_covered_through: hour + Duration::hours(i64::from(heartbeat_horizon_hours) + 1),
        absent_leaves,
    }))
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

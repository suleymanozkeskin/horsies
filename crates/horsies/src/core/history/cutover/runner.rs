//! Transaction/commit ownership for individual stages and the ordered driver.

use sqlx::PgPool;

use crate::broker::migrations::{expected_schema_version, successful_schema_version};
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{LIVE_TASKS, TASK_HISTORY_PARENT};

use super::drain::{verify_drained, DrainOutcome};
use super::identity::{
    attempts_identity_is_uuid, normalize_attempt_identity, AttemptIdentityNormalization,
};
use super::preflight::{run_preflight, CutoverPreflight, PreflightError, RelocationCoefficients};
use super::preparation::{
    prepare_legacy_batch, PreparationCursor, PreparationError, PreparationOutcome,
};
use super::program::{install_programs, uninstall_programs, ProgramInstallation, ProgramRollback};
use super::relocation::{
    relocate_terminal_batch, RelocationError, RelocationOutcome, RELOCATION_LEDGER,
};
use super::state::cutover_complete;
use super::tighten::{tighten_to_frozen, TightenOutcome};
use super::validation::{validate_cutover, ValidationOutcome};

#[derive(Debug, thiserror::Error)]
pub enum CutoverRunError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    History(#[from] HistoryError),
    #[error(transparent)]
    Preflight(#[from] PreflightError),
    #[error(transparent)]
    Preparation(#[from] PreparationError),
    #[error(transparent)]
    Relocation(#[from] RelocationError),
    #[error("cutover stage {stage} refused: {reasons}")]
    Refused {
        stage: &'static str,
        reasons: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PreparationSummary {
    pub rows_prepared: i64,
    pub live_rows_prepared: usize,
    pub inline_rows: usize,
    pub over_bound_rows: usize,
    pub policy_declined_rows: usize,
    pub decode_failed_rows: usize,
    pub batches_committed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RelocationSummary {
    pub rows_relocated: i64,
    pub legacy_kind_rows: i64,
    pub batches_committed: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CutoverRunOptions {
    pub coefficients: RelocationCoefficients,
    pub heartbeat_quiet_seconds: f64,
    pub retain_rerun_input_default: bool,
    pub preparation_batch_size: i64,
    pub relocation_batch_size: i64,
    pub backup_label: String,
    pub operator_confirmation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutoverStageReport {
    pub stage: &'static str,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutoverStatus {
    pub expected_schema_version: i64,
    pub stored_schema_version: Option<i64>,
    pub attested: bool,
    pub attempts_identity_uuid: bool,
    pub live_identity_uuid: bool,
    pub move_program_installed: bool,
    pub terminal_live_rows: i64,
    pub unprepared_live_rows: i64,
    pub history_rows: i64,
    pub relocation_ledger_rows: Option<i64>,
}

pub async fn stage_preflight(
    pool: &PgPool,
    coefficients: RelocationCoefficients,
) -> Result<CutoverPreflight, CutoverRunError> {
    let mut connection = pool.acquire().await?;
    Ok(run_preflight(&mut connection, coefficients).await?)
}

pub async fn stage_drain(
    pool: &PgPool,
    heartbeat_quiet_seconds: f64,
) -> Result<DrainOutcome, CutoverRunError> {
    let mut connection = pool.acquire().await?;
    Ok(verify_drained(&mut connection, heartbeat_quiet_seconds).await?)
}

pub async fn stage_normalize_identity(
    pool: &PgPool,
) -> Result<AttemptIdentityNormalization, CutoverRunError> {
    let mut transaction = pool.begin().await?;
    let outcome = normalize_attempt_identity(transaction.as_mut()).await?;
    match outcome {
        AttemptIdentityNormalization::Refused { .. } => transaction.rollback().await?,
        _ => transaction.commit().await?,
    }
    Ok(outcome)
}

pub async fn stage_install_programs(pool: &PgPool) -> Result<ProgramInstallation, CutoverRunError> {
    let mut transaction = pool.begin().await?;
    let outcome = install_programs(transaction.as_mut()).await?;
    match outcome {
        ProgramInstallation::Installed { .. } => transaction.commit().await?,
        ProgramInstallation::Refused { .. } => transaction.rollback().await?,
    }
    Ok(outcome)
}

pub async fn stage_rollback_programs(pool: &PgPool) -> Result<ProgramRollback, CutoverRunError> {
    let mut transaction = pool.begin().await?;
    let outcome = uninstall_programs(transaction.as_mut()).await?;
    match outcome {
        ProgramRollback::RolledBack { .. } => transaction.commit().await?,
        ProgramRollback::Refused { .. } => transaction.rollback().await?,
    }
    Ok(outcome)
}

pub async fn stage_prepare(
    pool: &PgPool,
    retain_default: bool,
    batch_size: i64,
) -> Result<PreparationSummary, CutoverRunError> {
    let mut cursor = PreparationCursor::start();
    let mut summary = PreparationSummary::default();
    loop {
        let mut transaction = pool.begin().await?;
        let outcome =
            prepare_legacy_batch(transaction.as_mut(), retain_default, batch_size, &cursor).await?;
        transaction.commit().await?;
        match outcome {
            PreparationOutcome::Batch {
                rows_prepared,
                live_rows_prepared,
                inline_rows,
                over_bound_rows,
                policy_declined_rows,
                decode_failed_rows,
                cursor: next_cursor,
            } => {
                summary.rows_prepared += rows_prepared as i64;
                summary.live_rows_prepared += live_rows_prepared;
                summary.inline_rows += inline_rows;
                summary.over_bound_rows += over_bound_rows;
                summary.policy_declined_rows += policy_declined_rows;
                summary.decode_failed_rows += decode_failed_rows;
                summary.batches_committed += 1;
                cursor = next_cursor;
            }
            PreparationOutcome::Complete { .. } => return Ok(summary),
        }
    }
}

pub async fn stage_relocate(
    pool: &PgPool,
    batch_size: i64,
) -> Result<RelocationSummary, CutoverRunError> {
    let mut summary = RelocationSummary::default();
    loop {
        let mut transaction = pool.begin().await?;
        let outcome = relocate_terminal_batch(transaction.as_mut(), batch_size).await?;
        transaction.commit().await?;
        match outcome {
            RelocationOutcome::Batch {
                rows_relocated,
                legacy_kind_rows,
                ..
            } => {
                summary.rows_relocated += rows_relocated as i64;
                summary.legacy_kind_rows += legacy_kind_rows;
                summary.batches_committed += 1;
            }
            RelocationOutcome::Complete {
                batches_committed,
                rows_relocated,
            } => {
                summary.batches_committed = batches_committed;
                summary.rows_relocated = rows_relocated;
                return Ok(summary);
            }
        }
    }
}

pub async fn stage_tighten(
    pool: &PgPool,
    backup_label: &str,
    operator_confirmation: &str,
) -> Result<TightenOutcome, CutoverRunError> {
    let mut transaction = pool.begin().await?;
    let outcome =
        tighten_to_frozen(transaction.as_mut(), backup_label, operator_confirmation).await?;
    match outcome {
        TightenOutcome::Complete { .. } => transaction.commit().await?,
        TightenOutcome::Refused { .. } => transaction.rollback().await?,
    }
    Ok(outcome)
}

pub async fn stage_validate(pool: &PgPool) -> Result<ValidationOutcome, CutoverRunError> {
    let mut transaction = pool.begin().await?;
    let outcome = validate_cutover(transaction.as_mut()).await?;
    transaction.commit().await?;
    Ok(outcome)
}

pub async fn read_status(pool: &PgPool) -> Result<CutoverStatus, CutoverRunError> {
    let mut connection = pool.acquire().await?;
    let attested = cutover_complete(&mut connection).await?;
    let attempts_identity_uuid = attempts_identity_is_uuid(&mut connection).await?;
    let live_identity_uuid: bool = sqlx::query_scalar(
        "SELECT atttypid = 'uuid'::regtype FROM pg_attribute
         WHERE attrelid = 'horsies_tasks'::regclass AND attname = 'id'",
    )
    .fetch_one(&mut *connection)
    .await?;
    let move_program_installed: bool = sqlx::query_scalar(
        "SELECT to_regprocedure(
             'horsies_move_task_to_history(uuid,text,text,timestamptz,text,text,text)'
         ) IS NOT NULL",
    )
    .fetch_one(&mut *connection)
    .await?;
    let terminal_live_rows: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {LIVE_TASKS}
         WHERE status NOT IN ('PENDING', 'CLAIMED', 'RUNNING')"
    ))
    .fetch_one(&mut *connection)
    .await?;
    let unprepared_live_rows: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {LIVE_TASKS}
         WHERE prepared_rerun_input_disposition IS NULL"
    ))
    .fetch_one(&mut *connection)
    .await?;
    let history_rows: i64 =
        sqlx::query_scalar(&format!("SELECT count(*) FROM {TASK_HISTORY_PARENT}"))
            .fetch_one(&mut *connection)
            .await?;
    let ledger_exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(RELOCATION_LEDGER)
        .fetch_one(&mut *connection)
        .await?;
    let relocation_ledger_rows = if ledger_exists {
        Some(
            sqlx::query_scalar(&format!(
                "SELECT COALESCE(sum(rows_relocated), 0) FROM {RELOCATION_LEDGER}"
            ))
            .fetch_one(&mut *connection)
            .await?,
        )
    } else {
        None
    };
    Ok(CutoverStatus {
        expected_schema_version: expected_schema_version(),
        stored_schema_version: successful_schema_version(pool)
            .await
            .map_err(|error| HistoryError::contract(error.to_string()))?,
        attested,
        attempts_identity_uuid,
        live_identity_uuid,
        move_program_installed,
        terminal_live_rows,
        unprepared_live_rows,
        history_rows,
        relocation_ledger_rows,
    })
}

pub async fn run_cutover(
    pool: &PgPool,
    options: &CutoverRunOptions,
) -> Result<Vec<CutoverStageReport>, CutoverRunError> {
    let mut reports = Vec::new();
    let preflight = stage_preflight(pool, options.coefficients).await?;
    reports.push(CutoverStageReport {
        stage: "preflight",
        detail: format!(
            "{} terminal rows; estimated {:.3}s, ceiling {:.3}s",
            preflight.terminal_live_rows,
            preflight.estimate.total_seconds,
            preflight.estimate.ceiling_seconds
        ),
    });

    match stage_drain(pool, options.heartbeat_quiet_seconds).await? {
        DrainOutcome::Verified { pending_rows } => reports.push(CutoverStageReport {
            stage: "drain",
            detail: format!("verified; {pending_rows} PENDING rows survive"),
        }),
        DrainOutcome::Blocked {
            claimed_rows,
            running_rows,
            finalizing_rows,
            recent_heartbeats,
        } => {
            return Err(CutoverRunError::Refused {
                stage: "drain",
                reasons: format!(
                    "claimed={claimed_rows}, running={running_rows}, \
                     finalizing={finalizing_rows}, recent_heartbeats={recent_heartbeats}"
                ),
            });
        }
    }

    match stage_normalize_identity(pool).await? {
        AttemptIdentityNormalization::AlreadyUuid => reports.push(CutoverStageReport {
            stage: "normalize-identity",
            detail: "attempt identity already uses uuid".to_owned(),
        }),
        AttemptIdentityNormalization::Converted => reports.push(CutoverStageReport {
            stage: "normalize-identity",
            detail: "attempt identity converted to uuid".to_owned(),
        }),
        AttemptIdentityNormalization::Refused { reasons } => {
            return Err(CutoverRunError::Refused {
                stage: "normalize-identity",
                reasons: reasons.join("; "),
            });
        }
    }
    match stage_install_programs(pool).await? {
        ProgramInstallation::Installed {
            statements_executed,
        } => reports.push(CutoverStageReport {
            stage: "install-programs",
            detail: format!("{statements_executed} statements executed"),
        }),
        ProgramInstallation::Refused { reasons } => {
            return Err(CutoverRunError::Refused {
                stage: "install-programs",
                reasons: reasons.join("; "),
            });
        }
    }

    let prepared = stage_prepare(
        pool,
        options.retain_rerun_input_default,
        options.preparation_batch_size,
    )
    .await?;
    reports.push(CutoverStageReport {
        stage: "prepare",
        detail: format!(
            "{} rows in {} batches; inline={}, over-bound={}, policy-declined={}, decode-failed={}",
            prepared.rows_prepared,
            prepared.batches_committed,
            prepared.inline_rows,
            prepared.over_bound_rows,
            prepared.policy_declined_rows,
            prepared.decode_failed_rows
        ),
    });
    let relocated = stage_relocate(pool, options.relocation_batch_size).await?;
    reports.push(CutoverStageReport {
        stage: "relocate",
        detail: format!(
            "{} rows in {} committed batches; {} LEGACY_TERMINAL",
            relocated.rows_relocated, relocated.batches_committed, relocated.legacy_kind_rows
        ),
    });
    match stage_tighten(pool, &options.backup_label, &options.operator_confirmation).await? {
        TightenOutcome::Complete {
            statements_executed,
        } => reports.push(CutoverStageReport {
            stage: "tighten",
            detail: format!("{statements_executed} statements executed"),
        }),
        TightenOutcome::Refused { reasons } => {
            return Err(CutoverRunError::Refused {
                stage: "tighten",
                reasons: reasons.join("; "),
            });
        }
    }
    match stage_validate(pool).await? {
        ValidationOutcome::Validated {
            history_rows,
            ledger_rows,
        } => reports.push(CutoverStageReport {
            stage: "validate",
            detail: format!("validated and attested; history={history_rows}, ledger={ledger_rows}"),
        }),
        ValidationOutcome::Invalid { violations } => {
            return Err(CutoverRunError::Refused {
                stage: "validate",
                reasons: violations.join("; "),
            });
        }
    }
    Ok(reports)
}

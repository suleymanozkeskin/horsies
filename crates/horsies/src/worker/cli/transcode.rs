//! `horsies transcode` command tree for the replacement-partition executor.

use clap::{Args, Subcommand, ValueEnum};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use crate::core::history::transcode::executor::{
    finalize_transcode, plan_transcode, run_copy_batch, swap_with_retries, verify_transcode,
};
use crate::core::history::transcode::jobs::lock_job;
use crate::core::history::transcode::maintenance::{
    active_maintenance_session, begin_transcode_maintenance, finish_transcode_maintenance,
};
use crate::core::history::transcode::outcomes::{
    ArchiveComponent, TranscodeCopyOutcome, TranscodePlanOutcome, TranscodeSwapOutcome,
};
use crate::core::history::transcode::TranscodeError;

#[derive(Debug, Args)]
pub struct TranscodeArgs {
    /// Direct PostgreSQL URL for the frozen schema-v35 database.
    #[arg(long, global = true)]
    pub database_url: Option<String>,

    #[command(subcommand)]
    pub command: TranscodeCommand,
}

#[derive(Debug, Subcommand)]
pub enum TranscodeCommand {
    /// Begin a real archive-maintenance session.
    Begin(SessionArgs),
    /// Inventory the component and create a durable reversible job.
    Plan(PlanArgs),
    /// Copy all remaining batches, committing each batch separately.
    Copy(JobBatchArgs),
    /// Verify full replacement content and record identity tokens.
    Verify(JobArgs),
    /// Drive bounded non-queuing swap attempts.
    Swap(JobArgs),
    /// Drop backup relations and retire the source decoder version.
    Finalize(JobArgs),
    /// End maintenance after every session job is complete.
    Finish(SessionArgs),
    /// Print durable job and maintenance facts.
    Status(JobArgs),
    /// Run begin, plan, copy, verify, swap, finalize, and finish in order.
    Run(RunArgs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ComponentArg {
    HistoryRow,
    Result,
    Attempts,
    RerunInput,
}

impl From<ComponentArg> for ArchiveComponent {
    fn from(value: ComponentArg) -> Self {
        match value {
            ComponentArg::HistoryRow => Self::HistoryRow,
            ComponentArg::Result => Self::Result,
            ComponentArg::Attempts => Self::Attempts,
            ComponentArg::RerunInput => Self::RerunInput,
        }
    }
}

#[derive(Debug, Args)]
pub struct SessionArgs {
    #[arg(long)]
    pub session_id: Uuid,
}

#[derive(Debug, Args)]
pub struct JobArgs {
    #[arg(long)]
    pub job_id: Uuid,
}

#[derive(Debug, Args)]
pub struct JobBatchArgs {
    #[arg(long)]
    pub job_id: Uuid,
    #[arg(long, default_value_t = 10_000)]
    pub batch_size: i64,
}

#[derive(Debug, Args)]
pub struct PlanArgs {
    #[arg(long)]
    pub job_id: Uuid,
    #[arg(long, value_enum)]
    pub component: ComponentArg,
    #[arg(long)]
    pub source_version: i16,
    #[arg(long)]
    pub target_version: i16,
    #[arg(long)]
    pub source_codec: String,
    #[arg(long)]
    pub target_codec: String,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    #[arg(long)]
    pub session_id: Uuid,
    #[arg(long)]
    pub job_id: Uuid,
    #[arg(long, value_enum)]
    pub component: ComponentArg,
    #[arg(long)]
    pub source_version: i16,
    #[arg(long)]
    pub target_version: i16,
    #[arg(long)]
    pub source_codec: String,
    #[arg(long)]
    pub target_codec: String,
    #[arg(long, default_value_t = 10_000)]
    pub batch_size: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum TranscodeCliError {
    #[error("--database-url is required for every transcode command")]
    MissingDatabaseUrl,
    #[error("transcode refused at {stage}: {reason}")]
    Refused { stage: &'static str, reason: String },
    #[error(transparent)]
    Transcode(#[from] TranscodeError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

async fn pool(args: &TranscodeArgs) -> Result<sqlx::PgPool, TranscodeCliError> {
    let url = args
        .database_url
        .as_deref()
        .ok_or(TranscodeCliError::MissingDatabaseUrl)?;
    Ok(PgPoolOptions::new().max_connections(4).connect(url).await?)
}

async fn begin(pool: &sqlx::PgPool, session_id: Uuid) -> Result<(), TranscodeCliError> {
    let mut transaction = pool.begin().await?;
    begin_transcode_maintenance(&mut transaction, session_id).await?;
    transaction.commit().await?;
    println!("archive maintenance active: session={session_id}");
    Ok(())
}

async fn plan(pool: &sqlx::PgPool, command: &PlanArgs) -> Result<(), TranscodeCliError> {
    let mut transaction = pool.begin().await?;
    let outcome = plan_transcode(
        &mut transaction,
        command.job_id,
        command.component.into(),
        command.source_version,
        command.target_version,
        &command.source_codec,
        &command.target_codec,
    )
    .await?;
    match outcome {
        TranscodePlanOutcome::Planned(plan) => {
            transaction.commit().await?;
            println!(
                "transcode planned: job={}, component={}, source-version={}, target-version={}, transformed={}, copied={}, relations={}, payload-before={}, payload-after={}, relation-bytes={}, peak-disk-budget={}, wal-budget={}, rollback-wal-budget={}, rollback-peak-disk-budget={}, reversible={}",
                plan.job_id,
                plan.component.as_str(),
                plan.source_version,
                plan.target_version,
                plan.transformed_rows,
                plan.copied_rows,
                plan.relation_count,
                plan.payload_bytes,
                plan.projected_payload_bytes,
                plan.affected_relation_bytes,
                plan.peak_additional_disk_budget_bytes,
                plan.wal_budget_bytes,
                plan.rollback_wal_budget_bytes,
                plan.rollback_peak_additional_disk_budget_bytes,
                plan.reversible,
            );
            Ok(())
        }
        TranscodePlanOutcome::Rejected(rejected) => Err(TranscodeCliError::Refused {
            stage: "plan",
            reason: format!(
                "component={}, affected={}, reason={}",
                rejected.component.as_str(),
                rejected.affected_rows,
                rejected.reason
            ),
        }),
    }
}

async fn copy(pool: &sqlx::PgPool, job_id: Uuid, batch_size: i64) -> Result<(), TranscodeCliError> {
    loop {
        let mut transaction = pool.begin().await?;
        let outcome = run_copy_batch(&mut transaction, job_id, batch_size).await?;
        match outcome {
            TranscodeCopyOutcome::Batch(batch) => {
                transaction.commit().await?;
                println!(
                    "transcode copy committed: job={}, relation={}, batch={}, rows={}, completed={}, total={}",
                    batch.job_id,
                    batch.relation_ordinal,
                    batch.batch_number,
                    batch.rows_copied,
                    batch.copied_rows_completed,
                    batch.copied_rows_total,
                );
            }
            TranscodeCopyOutcome::Ready(ready) => {
                transaction.commit().await?;
                println!(
                    "transcode copy complete: job={}, rows={}",
                    ready.job_id, ready.copied_rows_total
                );
                return Ok(());
            }
            TranscodeCopyOutcome::Rejected(rejected) => {
                return Err(TranscodeCliError::Refused {
                    stage: "copy",
                    reason: format!(
                        "job={}, relation={}, kind={}, observed={}",
                        rejected.job_id,
                        rejected.relation_ordinal,
                        rejected.kind.as_str(),
                        rejected.observed_rows,
                    ),
                });
            }
        }
    }
}

async fn verify(pool: &sqlx::PgPool, job_id: Uuid) -> Result<(), TranscodeCliError> {
    let mut transaction = pool.begin().await?;
    let report = verify_transcode(&mut transaction, job_id).await?;
    transaction.commit().await?;
    println!(
        "transcode verification: job={}, verified={}, source-changed={}, mismatches={}, invalid-targets={}, copied={}",
        report.job_id,
        report.verified,
        report.source_relations_changed,
        report.replacement_row_mismatches,
        report.invalid_target_rows,
        report.copied_rows_total,
    );
    if report.verified {
        Ok(())
    } else {
        Err(TranscodeCliError::Refused {
            stage: "verify",
            reason: "replacement content or identity did not verify".to_owned(),
        })
    }
}

async fn swap(pool: &sqlx::PgPool, job_id: Uuid) -> Result<(), TranscodeCliError> {
    match swap_with_retries(pool, job_id).await? {
        TranscodeSwapOutcome::Swapped(swapped) => {
            println!(
                "transcode swap complete: job={}, relations={}",
                swapped.job_id, swapped.relations_swapped
            );
            Ok(())
        }
        TranscodeSwapOutcome::Busy(busy) => Err(TranscodeCliError::Refused {
            stage: "swap",
            reason: format!(
                "lock-mode={}, relations={}",
                busy.lock_mode.as_str(),
                busy.relation_names.join(",")
            ),
        }),
        TranscodeSwapOutcome::Exhausted(exhausted) => {
            println!(
                "transcode swap exhausted: job={}, lock-mode={}, relations={}, attempts={}, retry-sleep={:.3}s, blockers={}, blocker-capture-failed={}",
                exhausted.job_id,
                exhausted.lock_mode.as_str(),
                exhausted.relation_names.join(","),
                exhausted.attempts,
                exhausted.retry_sleep_seconds,
                exhausted.blockers.len(),
                exhausted.blocker_capture_failed,
            );
            for blocker in exhausted.blockers {
                println!(
                    "swap blocker: pid={}, relation={}, held-mode={}, granted={}, state={}, age-seconds={}, wait-event={}, query={}",
                    blocker.pid,
                    blocker.relation_name,
                    blocker.held_lock_mode,
                    blocker.granted,
                    blocker.state.as_deref().unwrap_or("unknown"),
                    blocker
                        .transaction_age_seconds
                        .map(|value| format!("{value:.3}"))
                        .unwrap_or_else(|| "unknown".to_owned()),
                    blocker.wait_event.as_deref().unwrap_or("none"),
                    blocker.query.as_deref().unwrap_or("none"),
                );
            }
            Err(TranscodeCliError::Refused {
                stage: "swap",
                reason: "non-queuing lock retry ceiling reached".to_owned(),
            })
        }
    }
}

async fn finalize(pool: &sqlx::PgPool, job_id: Uuid) -> Result<(), TranscodeCliError> {
    let mut transaction = pool.begin().await?;
    let finalized = finalize_transcode(&mut transaction, job_id).await?;
    transaction.commit().await?;
    println!(
        "transcode finalized: job={}, retired-version={}, decoder-retirement-ready={}",
        finalized.job_id, finalized.retired_source_version, finalized.decoder_retirement_ready,
    );
    Ok(())
}

async fn finish(pool: &sqlx::PgPool, session_id: Uuid) -> Result<(), TranscodeCliError> {
    let mut transaction = pool.begin().await?;
    finish_transcode_maintenance(&mut transaction, session_id).await?;
    transaction.commit().await?;
    println!("archive maintenance complete: session={session_id}");
    Ok(())
}

async fn status(pool: &sqlx::PgPool, job_id: Uuid) -> Result<(), TranscodeCliError> {
    let mut transaction = pool.begin().await?;
    let job = lock_job(&mut transaction, job_id).await?;
    let active = active_maintenance_session(&mut transaction).await?;
    transaction.rollback().await?;
    println!(
        "transcode status: job={}, state={}, component={}, source-version={}, target-version={}, copied={}, total={}, relations={}, maintenance-session={}, active-session={}, wal-bytes={}",
        job.job_id,
        job.state.as_str(),
        job.component.as_str(),
        job.source_version,
        job.target_version,
        job.copied_rows_completed,
        job.copied_rows_total,
        job.relation_count,
        job.maintenance_session_id,
        active.map(|value| value.to_string()).unwrap_or_else(|| "none".to_owned()),
        job.wal_bytes.map(|value| value.to_string()).unwrap_or_else(|| "pending".to_owned()),
    );
    Ok(())
}

pub async fn execute_transcode(args: TranscodeArgs) -> Result<(), TranscodeCliError> {
    let pool = pool(&args).await?;
    match args.command {
        TranscodeCommand::Begin(command) => begin(&pool, command.session_id).await,
        TranscodeCommand::Plan(command) => plan(&pool, &command).await,
        TranscodeCommand::Copy(command) => copy(&pool, command.job_id, command.batch_size).await,
        TranscodeCommand::Verify(command) => verify(&pool, command.job_id).await,
        TranscodeCommand::Swap(command) => swap(&pool, command.job_id).await,
        TranscodeCommand::Finalize(command) => finalize(&pool, command.job_id).await,
        TranscodeCommand::Finish(command) => finish(&pool, command.session_id).await,
        TranscodeCommand::Status(command) => status(&pool, command.job_id).await,
        TranscodeCommand::Run(command) => {
            begin(&pool, command.session_id).await?;
            plan(
                &pool,
                &PlanArgs {
                    job_id: command.job_id,
                    component: command.component,
                    source_version: command.source_version,
                    target_version: command.target_version,
                    source_codec: command.source_codec,
                    target_codec: command.target_codec,
                },
            )
            .await?;
            copy(&pool, command.job_id, command.batch_size).await?;
            verify(&pool, command.job_id).await?;
            swap(&pool, command.job_id).await?;
            finalize(&pool, command.job_id).await?;
            finish(&pool, command.session_id).await
        }
    }
}

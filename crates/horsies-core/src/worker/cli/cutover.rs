//! `horsies cutover` command tree and direct operator-facing reports.

use std::collections::HashSet;
use std::str::FromStr;

use clap::{Args, Subcommand, ValueEnum};
use sqlx::postgres::PgPoolOptions;

use crate::core::history::cutover::drain::DrainOutcome;
use crate::core::history::cutover::ladder::{
    evaluate_rung, BatchCommit, LadderError, MeasuredRun, RungOutcome, LADDER,
};
use crate::core::history::cutover::preflight::RelocationCoefficients;
use crate::core::history::cutover::program::ProgramInstallation;
use crate::core::history::cutover::runner::{
    read_status, run_cutover, stage_drain, stage_install_programs, stage_normalize_identity,
    stage_preflight, stage_prepare, stage_relocate, stage_rollback_programs, stage_tighten,
    stage_validate, CutoverRunError, CutoverRunOptions,
};
use crate::core::history::cutover::tighten::TightenOutcome;
use crate::core::history::cutover::validation::ValidationOutcome;

#[derive(Debug, Args)]
pub struct CutoverArgs {
    /// Direct PostgreSQL URL. Required for every database-backed stage.
    #[arg(long, global = true)]
    pub database_url: Option<String>,

    #[command(subcommand)]
    pub command: CutoverCommand,
}

#[derive(Debug, Subcommand)]
pub enum CutoverCommand {
    /// Inventory the emitted schema and calculate a bounded total window.
    Preflight(CoefficientArgs),
    /// Judge one measured ladder rung against the ruled bounds and refit it.
    LadderEvaluate(LadderEvaluateArgs),
    /// Verify that no old-fleet work remains in flight.
    Drain(DrainArgs),
    /// Normalize attempts identity and replace the drained fleet's programs.
    InstallPrograms(InstallProgramsArgs),
    /// Prepare legacy enqueue facts in committing keyset batches.
    Prepare(PrepareArgs),
    /// Relocate terminal rows in committing, ledgered batches.
    Relocate(RelocateArgs),
    /// Cross the point of no return using an exact named-backup confirmation.
    Tighten(TightenArgs),
    /// Validate the frozen shape and write or revoke the attestation.
    Validate,
    /// Execute R2: restore the chain-owned pre-cutover program.
    RollbackPrograms,
    /// Report durable and structural stage state without mutating it.
    Status,
    /// Run the full documented stage order.
    Run(RunArgs),
}

#[derive(Debug, Clone, Args)]
pub struct CoefficientArgs {
    #[arg(long)]
    pub relocation_seconds_per_million: f64,
    #[arg(long)]
    pub fixed_seconds: f64,
    #[arg(long)]
    pub preparation_seconds_per_million: f64,
}

impl CoefficientArgs {
    fn build(&self) -> Result<RelocationCoefficients, CutoverCliError> {
        Ok(RelocationCoefficients::new(
            self.relocation_seconds_per_million,
            self.fixed_seconds,
            self.preparation_seconds_per_million,
        )?)
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LadderRungName {
    OneMillion,
    TenMillion,
    HundredMillion,
}

impl LadderRungName {
    fn index(self) -> usize {
        match self {
            Self::OneMillion => 0,
            Self::TenMillion => 1,
            Self::HundredMillion => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BatchCommitArg(BatchCommit);

impl FromStr for BatchCommitArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (rows, seconds) = value
            .split_once(':')
            .ok_or_else(|| "commit must be ROWS:SECONDS".to_owned())?;
        Ok(Self(BatchCommit {
            cumulative_rows: rows
                .parse()
                .map_err(|_| "commit rows must be an integer".to_owned())?,
            elapsed_seconds: seconds
                .parse()
                .map_err(|_| "commit seconds must be a number".to_owned())?,
        }))
    }
}

#[derive(Debug, Args)]
pub struct LadderEvaluateArgs {
    #[command(flatten)]
    pub coefficients: CoefficientArgs,
    #[arg(long, value_enum)]
    pub rung: LadderRungName,
    #[arg(long)]
    pub measured_seconds: f64,
    #[arg(long)]
    pub measured_fixed_seconds: f64,
    #[arg(long)]
    pub measured_preparation_seconds: f64,
    /// One cumulative `ROWS:SECONDS` observation; repeat at least twice.
    #[arg(long = "commit", required = true)]
    pub commits: Vec<BatchCommitArg>,
}

#[derive(Debug, Args)]
pub struct DrainArgs {
    #[arg(long, default_value_t = 60.0)]
    pub heartbeat_quiet_seconds: f64,
}

#[derive(Debug, Args)]
pub struct InstallProgramsArgs {
    #[arg(long, default_value_t = 60.0)]
    pub heartbeat_quiet_seconds: f64,
}

#[derive(Debug, Args)]
pub struct PrepareArgs {
    #[arg(long, action = clap::ArgAction::Set, default_value_t = false)]
    pub retain_rerun_input_default: bool,
    #[arg(long, default_value_t = 10_000)]
    pub batch_size: i64,
}

#[derive(Debug, Args)]
pub struct RelocateArgs {
    #[arg(long, default_value_t = 10_000)]
    pub batch_size: i64,
}

#[derive(Debug, Args)]
pub struct TightenArgs {
    #[arg(long)]
    pub backup_label: String,
    #[arg(long)]
    pub operator_confirmation: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
pub enum RunStageConfirmation {
    Drain,
    NormalizeIdentity,
    InstallPrograms,
    Prepare,
    Relocate,
    Tighten,
    Validate,
}

impl RunStageConfirmation {
    const fn operator_label(self) -> &'static str {
        match self {
            Self::Drain => "drain",
            Self::NormalizeIdentity => "normalize-identity",
            Self::InstallPrograms => "install-programs",
            Self::Prepare => "prepare",
            Self::Relocate => "relocate",
            Self::Tighten => "tighten",
            Self::Validate => "validate",
        }
    }
}

const REQUIRED_RUN_CONFIRMATIONS: [RunStageConfirmation; 7] = [
    RunStageConfirmation::Drain,
    RunStageConfirmation::NormalizeIdentity,
    RunStageConfirmation::InstallPrograms,
    RunStageConfirmation::Prepare,
    RunStageConfirmation::Relocate,
    RunStageConfirmation::Tighten,
    RunStageConfirmation::Validate,
];

#[derive(Debug, Args)]
pub struct RunArgs {
    #[command(flatten)]
    pub coefficients: CoefficientArgs,
    #[arg(long, default_value_t = 60.0)]
    pub heartbeat_quiet_seconds: f64,
    #[arg(long, action = clap::ArgAction::Set, default_value_t = false)]
    pub retain_rerun_input_default: bool,
    #[arg(long, default_value_t = 10_000)]
    pub preparation_batch_size: i64,
    #[arg(long, default_value_t = 10_000)]
    pub relocation_batch_size: i64,
    #[arg(long)]
    pub backup_label: String,
    #[arg(long)]
    pub operator_confirmation: String,
    /// Confirm each mutating stage explicitly; repeat for all documented stages.
    #[arg(long = "confirm-stage", value_enum, required = true)]
    pub confirmations: Vec<RunStageConfirmation>,
}

#[derive(Debug, thiserror::Error)]
pub enum CutoverCliError {
    #[error("--database-url is required for this cutover command")]
    MissingDatabaseUrl,
    #[error("run is missing explicit confirmations for: {0}")]
    MissingConfirmations(String),
    #[error("ladder rung {rung} stopped: {reason}")]
    LadderStopped {
        rung: &'static str,
        reason: &'static str,
    },
    #[error(transparent)]
    Run(#[from] CutoverRunError),
    #[error(transparent)]
    Preflight(#[from] crate::core::history::cutover::preflight::PreflightError),
    #[error(transparent)]
    Ladder(#[from] LadderError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

async fn pool(args: &CutoverArgs) -> Result<sqlx::PgPool, CutoverCliError> {
    let url = args
        .database_url
        .as_deref()
        .ok_or(CutoverCliError::MissingDatabaseUrl)?;
    Ok(PgPoolOptions::new().max_connections(2).connect(url).await?)
}

fn print_drain(outcome: &DrainOutcome) -> Result<(), String> {
    match outcome {
        DrainOutcome::Verified { pending_rows } => {
            println!("drain verified: {pending_rows} PENDING rows survive cutover");
            Ok(())
        }
        DrainOutcome::Blocked {
            claimed_rows,
            running_rows,
            finalizing_rows,
            recent_heartbeats,
        } => {
            let facts = format!(
                "claimed={claimed_rows}, running={running_rows}, \
                 finalizing={finalizing_rows}, recent_heartbeats={recent_heartbeats}"
            );
            println!("drain blocked: {facts}");
            Err(facts)
        }
    }
}

fn require_run_confirmations(command: &RunArgs) -> Result<(), CutoverCliError> {
    let supplied: HashSet<_> = command.confirmations.iter().copied().collect();
    let missing: Vec<String> = REQUIRED_RUN_CONFIRMATIONS
        .iter()
        .filter(|stage| !supplied.contains(stage))
        .map(|stage| stage.operator_label().to_owned())
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(CutoverCliError::MissingConfirmations(missing.join(", ")))
    }
}

pub async fn execute_cutover(args: CutoverArgs) -> Result<(), CutoverCliError> {
    if let CutoverCommand::LadderEvaluate(command) = &args.command {
        let rung = LADDER[command.rung.index()];
        let measured = MeasuredRun {
            rows: rung.rows,
            seconds: command.measured_seconds,
            fixed_seconds: command.measured_fixed_seconds,
            preparation_seconds: command.measured_preparation_seconds,
            commits: command.commits.iter().map(|commit| commit.0).collect(),
        };
        match evaluate_rung(rung, command.coefficients.build()?, &measured)? {
            RungOutcome::Passed {
                estimate, refit, ..
            } => println!(
                "ladder rung {} passed: measured={:.3}s estimate={:.3}s \
                 ceiling={:.3}s refit_slope={:.6}s/M fixed={:.3}s \
                 regression_intercept={:.3}s",
                rung.name,
                measured.seconds,
                estimate.total_seconds,
                estimate.ceiling_seconds,
                refit.coefficients.seconds_per_million_rows(),
                refit.coefficients.fixed_seconds(),
                refit.regression_intercept_seconds
            ),
            RungOutcome::Busted { estimate, .. } => {
                println!(
                    "ladder rung {} busted the ceiling: measured={:.3}s ceiling={:.3}s",
                    rung.name, measured.seconds, estimate.ceiling_seconds
                );
                return Err(CutoverCliError::LadderStopped {
                    rung: rung.name,
                    reason: "measured time exceeded the planning ceiling",
                });
            }
            RungOutcome::Overpredicted { estimate, .. } => {
                println!(
                    "ladder rung {} disproved the estimate from below: measured={:.3}s estimate={:.3}s",
                    rung.name, measured.seconds, estimate.total_seconds
                );
                return Err(CutoverCliError::LadderStopped {
                    rung: rung.name,
                    reason: "measured time fell below the prediction floor",
                });
            }
        }
        return Ok(());
    }
    if let CutoverCommand::Run(command) = &args.command {
        require_run_confirmations(command)?;
    }

    let pool = pool(&args).await?;
    match args.command {
        CutoverCommand::Preflight(coefficients) => {
            let report = stage_preflight(&pool, coefficients.build()?).await?;
            println!(
                "preflight ready: schema={}, terminal={}, legacy-kind={}, unfingerprinted={}, unprepared={}, \
                 unclassified={} ({} bytes), class-days={}, workflows={}, heartbeats={}, estimate={:.3}s, \
                 ceiling={:.3}s",
                report.stored_schema_version,
                report.terminal_live_rows,
                report.unrecorded_kind_rows,
                report.unfingerprinted_rows,
                report.unprepared_envelope_rows,
                report.unclassified_rows,
                report.unclassified_live_bytes,
                report.class_day_pairs,
                report.workflow_rows,
                report.heartbeat_rows,
                report.estimate.total_seconds,
                report.estimate.ceiling_seconds
            );
            for advisory in report.advisories {
                println!("advisory: {advisory}");
            }
        }
        CutoverCommand::Drain(command) => {
            let outcome = stage_drain(&pool, command.heartbeat_quiet_seconds).await?;
            if let Err(reasons) = print_drain(&outcome) {
                return Err(CutoverCliError::Run(CutoverRunError::Refused {
                    stage: "drain",
                    reasons,
                }));
            }
        }
        CutoverCommand::InstallPrograms(command) => {
            let drained = stage_drain(&pool, command.heartbeat_quiet_seconds).await?;
            if let Err(reasons) = print_drain(&drained) {
                return Err(CutoverCliError::Run(CutoverRunError::Refused {
                    stage: "drain",
                    reasons,
                }));
            }
            let identity = stage_normalize_identity(&pool).await?;
            match identity {
                crate::core::history::cutover::identity::AttemptIdentityNormalization::AlreadyUuid => {
                    println!("attempt identity already uses uuid");
                }
                crate::core::history::cutover::identity::AttemptIdentityNormalization::Converted => {
                    println!("attempt identity converted to uuid");
                }
                crate::core::history::cutover::identity::AttemptIdentityNormalization::Refused { reasons } => {
                    return Err(CutoverCliError::Run(CutoverRunError::Refused {
                        stage: "normalize-identity",
                        reasons: reasons.join("; "),
                    }));
                }
            }
            match stage_install_programs(&pool).await? {
                ProgramInstallation::Installed {
                    statements_executed,
                } => println!("programs installed: {statements_executed} statements"),
                ProgramInstallation::Refused { reasons } => {
                    return Err(CutoverCliError::Run(CutoverRunError::Refused {
                        stage: "install-programs",
                        reasons: reasons.join("; "),
                    }));
                }
            }
        }
        CutoverCommand::Prepare(command) => {
            let summary = stage_prepare(
                &pool,
                command.retain_rerun_input_default,
                command.batch_size,
            )
            .await?;
            println!(
                "preparation complete: rows={}, live={}, batches={}, inline={}, over-bound={}, policy-declined={}, decode-failed={}",
                summary.rows_prepared,
                summary.live_rows_prepared,
                summary.batches_committed,
                summary.inline_rows,
                summary.over_bound_rows,
                summary.policy_declined_rows,
                summary.decode_failed_rows,
            );
        }
        CutoverCommand::Relocate(command) => {
            let summary = stage_relocate(&pool, command.batch_size).await?;
            println!(
                "relocation complete: rows={}, batches={}, legacy-kind={}",
                summary.rows_relocated, summary.batches_committed, summary.legacy_kind_rows,
            );
        }
        CutoverCommand::Tighten(command) => {
            match stage_tighten(&pool, &command.backup_label, &command.operator_confirmation)
                .await?
            {
                TightenOutcome::Complete {
                    statements_executed,
                } => println!("tighten complete: {statements_executed} statements"),
                TightenOutcome::Refused { reasons } => {
                    for reason in &reasons {
                        println!("tighten refused: {reason}");
                    }
                    return Err(CutoverCliError::Run(CutoverRunError::Refused {
                        stage: "tighten",
                        reasons: reasons.join("; "),
                    }));
                }
            }
        }
        CutoverCommand::Validate => match stage_validate(&pool).await? {
            ValidationOutcome::Validated {
                history_rows,
                ledger_rows,
            } => println!(
                "validation passed and attested: history={history_rows}, ledger={ledger_rows}"
            ),
            ValidationOutcome::Invalid { violations } => {
                for violation in &violations {
                    println!("validation failed: {violation}");
                }
                return Err(CutoverCliError::Run(CutoverRunError::Refused {
                    stage: "validate",
                    reasons: violations.join("; "),
                }));
            }
        },
        CutoverCommand::RollbackPrograms => {
            let report = stage_rollback_programs(&pool).await?;
            match report {
                crate::core::history::cutover::program::ProgramRollback::RolledBack {
                    teardown_statements_executed,
                    attempt_identity,
                } => println!(
                    "program rollback complete: teardown-statements={teardown_statements_executed}, attempt-identity={}; the v26 in-place program is installed",
                    match attempt_identity {
                        crate::core::history::cutover::identity::AttemptIdentityRestoration::AlreadyVarchar => "varchar-already-restored",
                        crate::core::history::cutover::identity::AttemptIdentityRestoration::Restored => "varchar-restored",
                        crate::core::history::cutover::identity::AttemptIdentityRestoration::Refused { .. } => unreachable!("refused identity restoration is returned as a refused program rollback"),
                    }
                ),
                crate::core::history::cutover::program::ProgramRollback::Refused { reasons } => {
                    return Err(CutoverCliError::Run(CutoverRunError::Refused {
                        stage: "rollback-programs",
                        reasons: reasons.join("; "),
                    }));
                }
            }
        }
        CutoverCommand::Status => {
            let status = read_status(&pool).await?;
            println!(
                "cutover status: expected-schema={}, stored-schema={}, attested={}, attempts-uuid={}, live-uuid={}, move-program={}, terminal-live={}, unprepared-live={}, history={}, ledger={}",
                status.expected_schema_version,
                status.stored_schema_version.map_or_else(|| "absent".to_owned(), |value| value.to_string()),
                status.attested,
                status.attempts_identity_uuid,
                status.live_identity_uuid,
                status.move_program_installed,
                status.terminal_live_rows,
                status.unprepared_live_rows,
                status.history_rows,
                status.relocation_ledger_rows.map_or_else(|| "absent".to_owned(), |value| value.to_string()),
            );
        }
        CutoverCommand::Run(command) => {
            let options = CutoverRunOptions {
                coefficients: command.coefficients.build()?,
                heartbeat_quiet_seconds: command.heartbeat_quiet_seconds,
                retain_rerun_input_default: command.retain_rerun_input_default,
                preparation_batch_size: command.preparation_batch_size,
                relocation_batch_size: command.relocation_batch_size,
                backup_label: command.backup_label,
                operator_confirmation: command.operator_confirmation,
            };
            for report in run_cutover(&pool, &options).await? {
                println!("{}: {}", report.stage, report.detail);
            }
        }
        CutoverCommand::LadderEvaluate(_) => unreachable!("handled without a database"),
    }
    Ok(())
}

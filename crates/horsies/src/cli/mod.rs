//! The CLI runner of the user's binary: `horsies::cli::run(app)` serves
//! child mode first, then `worker`, `scheduler` and `check`.
//!
//! ```ignore
//! fn main() -> std::process::ExitCode {
//!     let app = build_app(); // config, registrations, provide(...), on_child_start(...)
//!     horsies::cli::run(app)
//! }
//! ```
//!
//! - `<binary> worker --profile <name> [--loglevel LEVEL]`: Python
//!   `horsies worker`.
//! - `<binary> scheduler [--loglevel LEVEL]`: Python `horsies scheduler`.
//! - `<binary> check [--live] [--loglevel LEVEL]`: Python `horsies check`.
//!
//! The worker starts this binary again for each child; `run` sees the child
//! mode argument first and serves the child. Every line of `main` before
//! `run` runs in each child too.
//!
//! Exit codes: 0 on an operator stop (`SIGTERM`, `SIGINT`) and on a passing
//! check; 1 on any other end; 2 for arguments that do not parse or a
//! removed flag; a child exits with the code of `ChildHostExit`.

// The shipped binary's commands (`crate::tools`) log through it too.
pub(crate) mod logging;
mod removed_flags;

use std::ffi::OsString;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use horsies_core::process_runtime::process_runtime;
use horsies_core::{ChildHostExit, ChildLogSetup, ChildMode};

use crate::child::exit_byte;
use crate::{
    AppRole, CheckPassed, CheckReport, Horsies, SchedulerRunError, WorkerLogLevel, WorkerRunError,
    WorkerStart,
};

/// The exit status of an argument error, as argparse and clap give it.
const USAGE_EXIT: u8 = 2;

/// Run the command of this process's arguments for `app`, or serve as a
/// worker child: see the module docs. Blocks the calling thread until the
/// command ends, so call it from a plain `fn main`; inside a tokio runtime
/// it prints why and returns exit status 1.
pub fn run(app: Horsies) -> ExitCode {
    match crate::child_mode() {
        ChildMode::Child(args) => {
            return app.serve_child_with_logs(
                args,
                ChildLogSetup::Install(Box::new(logging::install_child_logs)),
            )
        }
        ChildMode::BadArguments(error) => {
            eprintln!("horsies: child mode cannot start: {error}");
            return ExitCode::from(exit_byte(ChildHostExit::BadArguments.code()));
        }
        ChildMode::Worker => {}
    }
    if tokio::runtime::Handle::try_current().is_ok() {
        eprintln!(
            "horsies: cli::run blocks its thread until the command ends; call it from a plain \
             fn main, not inside a tokio runtime"
        );
        return ExitCode::FAILURE;
    }
    let arguments: Vec<OsString> = std::env::args_os().collect();
    if let Some(refusal) = removed_flags::refusal(&arguments) {
        eprintln!("error: {refusal}");
        return ExitCode::from(USAGE_EXIT);
    }
    let cli = match Cli::try_parse_from(&arguments) {
        Ok(cli) => cli,
        Err(error) => {
            let _printed = error.print();
            return ExitCode::from(u8::try_from(error.exit_code()).unwrap_or(USAGE_EXIT));
        }
    };
    run_command(app, cli.command)
}

/// The commands of the runner.
#[derive(Debug, Parser)]
#[command(about = "Run the horsies worker, the scheduler or the check of this app")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start a worker of one profile of AppConfig.worker_profiles.
    Worker {
        /// The worker profile: it holds the worker's settings (children,
        /// recycling, claim limits, drain timeout). Required.
        #[arg(long)]
        profile: Option<String>,
        #[arg(long, value_enum, ignore_case = true, default_value = "INFO")]
        loglevel: LogLevel,
    },
    /// Start the scheduler of AppConfig.schedule.
    Scheduler {
        #[arg(long, value_enum, ignore_case = true, default_value = "INFO")]
        loglevel: LogLevel,
    },
    /// Check the app without starting a service.
    Check {
        /// Also connect to the database and compare each worker profile
        /// with the server's connection limit.
        #[arg(long)]
        live: bool,
        #[arg(long, value_enum, ignore_case = true, default_value = "WARNING")]
        loglevel: LogLevel,
    },
}

/// The `--loglevel` values, Python's names. The shipped binary's `web`
/// takes them too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "UPPER")]
pub(crate) enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

impl LogLevel {
    /// The level of the worker's children.
    pub(crate) fn child_level(self) -> WorkerLogLevel {
        match self {
            Self::Debug => WorkerLogLevel::Debug,
            Self::Info => WorkerLogLevel::Info,
            Self::Warning => WorkerLogLevel::Warning,
            Self::Error => WorkerLogLevel::Error,
            Self::Critical => WorkerLogLevel::Critical,
        }
    }
}

/// Install the logs, then run `command`. Returns the exit status.
fn run_command(mut app: Horsies, command: Command) -> ExitCode {
    match command {
        Command::Worker { profile, loglevel } => {
            logging::install_logs(loglevel.child_level());
            let start = match profile {
                Some(name) => WorkerStart::profile(name).with_log_level(loglevel.child_level()),
                None => WorkerStart::without_profile(loglevel.child_level()),
            };
            block_on_process_runtime(app.run_worker(start)).map_or(ExitCode::FAILURE, worker_exit)
        }
        Command::Scheduler { loglevel } => {
            logging::install_logs(loglevel.child_level());
            app.set_role(AppRole::Scheduler);
            block_on_process_runtime(app.run_scheduler()).map_or(ExitCode::FAILURE, scheduler_exit)
        }
        Command::Check { live, loglevel } => {
            logging::install_logs(loglevel.child_level());
            let checked = match live {
                false => app.check(),
                true => match block_on_process_runtime(app.check_live()) {
                    Some(checked) => checked,
                    None => return ExitCode::FAILURE,
                },
            };
            check_exit(checked)
        }
    }
}

/// Run `future` to its end on the core's process runtime from this thread.
/// `None` when the runtime cannot start; the cause is printed.
fn block_on_process_runtime<F: std::future::Future>(future: F) -> Option<F::Output> {
    match process_runtime() {
        Ok(runtime) => Some(runtime.runtime().block_on(future)),
        Err(refusal) => {
            eprintln!("horsies: {refusal}");
            None
        }
    }
}

/// The exit status of a worker run, with its log line (Python
/// `_exit_code_of`): a report goes to stderr, a failure to the log.
fn worker_exit(end: Result<crate::WorkerStopped, WorkerRunError>) -> ExitCode {
    match end {
        Ok(crate::WorkerStopped) => {
            tracing::info!("worker stopped");
            ExitCode::SUCCESS
        }
        Err(WorkerRunError::CheckRefused(report)) => print_report(&report),
        Err(error @ WorkerRunError::ProfileRefused { .. }) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
        Err(
            error @ (WorkerRunError::ManifestBound { .. }
            | WorkerRunError::ChildCommandRefused { .. }
            | WorkerRunError::ConnectionSettingsRefused { .. }
            | WorkerRunError::RuntimeUnavailable(_)
            | WorkerRunError::BootFailed { .. }
            | WorkerRunError::Failed { .. }),
        ) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}

/// The exit status of a scheduler run, with its log line.
fn scheduler_exit(end: Result<crate::SchedulerStopped, SchedulerRunError>) -> ExitCode {
    match end {
        Ok(crate::SchedulerStopped) => {
            tracing::info!("scheduler stopped");
            ExitCode::SUCCESS
        }
        Err(SchedulerRunError::CheckRefused(report)) => print_report(&report),
        Err(
            error @ (SchedulerRunError::NotConfigured
            | SchedulerRunError::Disabled
            | SchedulerRunError::ManifestRefused { .. }
            | SchedulerRunError::StartRefused { .. }
            | SchedulerRunError::StartFailed { .. }
            | SchedulerRunError::RuntimeUnavailable(_)),
        ) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}

/// The exit status of a check: the success text on stdout, or the report on
/// stderr (Python `check_command`).
fn check_exit(checked: Result<CheckPassed, CheckReport>) -> ExitCode {
    match checked {
        Ok(passed) => {
            println!("{passed}");
            ExitCode::SUCCESS
        }
        Err(report) => print_report(&report),
    }
}

fn print_report(report: &CheckReport) -> ExitCode {
    eprintln!("{report}");
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(arguments)
    }

    #[test]
    fn worker_takes_a_profile_and_a_level_in_any_case() {
        let cli = parse(&["app", "worker", "--profile", "web", "--loglevel", "debug"]).unwrap();
        match cli.command {
            Command::Worker { profile, loglevel } => {
                assert_eq!(profile.as_deref(), Some("web"));
                assert_eq!(loglevel, LogLevel::Debug);
            }
            other => panic!("expected worker, got {other:?}"),
        }
    }

    #[test]
    fn each_command_has_python_s_default_level() {
        let Command::Worker { profile, loglevel } = parse(&["app", "worker"]).unwrap().command
        else {
            panic!("expected worker");
        };
        assert_eq!((profile, loglevel), (None, LogLevel::Info));
        let Command::Check { live, loglevel } = parse(&["app", "check"]).unwrap().command else {
            panic!("expected check");
        };
        assert_eq!((live, loglevel), (false, LogLevel::Warning));
        assert!(matches!(
            parse(&["app", "scheduler"]).unwrap().command,
            Command::Scheduler {
                loglevel: LogLevel::Info
            }
        ));
    }

    #[test]
    fn an_unknown_level_and_an_unknown_command_are_usage_errors() {
        for arguments in [
            &["app", "worker", "--loglevel", "TRACE"][..],
            &["app", "web"][..],
            &["app"][..],
        ] {
            let error = parse(arguments).unwrap_err();
            assert_eq!(error.exit_code(), 2, "{arguments:?}");
        }
    }

    #[test]
    fn critical_logs_at_the_child_level_critical() {
        assert_eq!(LogLevel::Critical.child_level(), WorkerLogLevel::Critical);
        assert_eq!(LogLevel::Warning.child_level(), WorkerLogLevel::Warning);
    }
}

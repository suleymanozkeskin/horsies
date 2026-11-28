//! The CLI runner of the user's binary: `horsies::cli::run(app)` serves
//! child mode first, then `worker`, `scheduler`, `check` and `metrics`.
//!
//! ```ignore
//! fn main() -> std::process::ExitCode {
//!     let app = build_app(); // config, registrations, provide(...), on_child_start(...)
//!     horsies::cli::run(app)
//! }
//! ```
//!
//! - `<binary> worker --profile <name> [--log-level LEVEL]
//!   [--metrics-listen HOST:PORT]`: Python `horsies worker`. With
//!   `--metrics-listen`, the worker serves its metrics at `GET /metrics` and
//!   its health probes at `GET /livez` and `GET /readyz`; there is no default
//!   address.
//! - `<binary> scheduler [--log-level LEVEL]`: Python `horsies scheduler`.
//! - `<binary> check [--live] [--log-level LEVEL]`: Python `horsies check`.
//! - `<binary> metrics --metrics-listen HOST:PORT [--log-level LEVEL]`:
//!   Python `horsies metrics`; the fleet metrics of the app's database at
//!   `GET /metrics`. `--metrics-listen` has no default.
//!
//! The worker starts this binary again for each child; `run` sees the child
//! mode argument first and serves the child. Every line of `main` before
//! `run` runs in each child too.
//!
//! The logs go to stderr as text. [`run_with_log_format`] with
//! [`LogFormat::JsonLogs`] writes one JSON object per record instead, in the
//! runner and in each child.
//!
//! Exit codes: 0 on an operator stop (`SIGTERM`, `SIGINT`) and on a passing
//! check; 1 on any other end; 2 for arguments that do not parse or a
//! removed flag; a child exits with the code of `ChildHostExit`.

// The shipped binary's commands (`crate::tools`) log through it too.
pub(crate) mod logging;
mod metrics;
mod removed_flags;

use std::ffi::OsString;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use horsies_core::telemetry::listen::ListenAddress;
use horsies_core::{ChildHostExit, ChildLogSetup, ChildMode};

use crate::child::exit_byte;
use crate::process_state::runtime_for;
use crate::{
    AppRole, CheckPassed, Horsies, SchedulerRunError, ValidationReport, WorkerLogLevel,
    WorkerRunError, WorkerRunOptions,
};

/// The exit status of an argument error, as argparse and clap give it.
const USAGE_EXIT: u8 = 2;

/// The format of the log records of the runner and of the worker's
/// children (Python `TextLogs`, `JsonLogs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// One line of text per record, with ANSI color on a terminal. The
    /// format of [`run`].
    TextLogs,
    /// One JSON object per line: `timestamp`, `level`, `target`, the fields
    /// of the event at the top level (`message` among them), `span` (the
    /// innermost span, with its name and fields) and `spans` (every span of
    /// the record, outermost first).
    JsonLogs,
}

/// Run the command of this process's arguments for `app`, or serve as a
/// worker child: see the module docs. Blocks the calling thread until the
/// command ends, so call it from a plain `fn main`; inside a tokio runtime
/// it prints why and returns exit status 1. The logs are text
/// ([`LogFormat::TextLogs`]).
pub fn run(app: Horsies) -> ExitCode {
    run_with_log_format(app, LogFormat::TextLogs)
}

/// [`run`] with the logs in `format`. `main` runs in each worker child
/// too, so the children log in the same format.
pub fn run_with_log_format(app: Horsies, format: LogFormat) -> ExitCode {
    match crate::child_mode() {
        ChildMode::Child(args) => {
            return app.serve_child_with_logs(
                args,
                ChildLogSetup::Install(Box::new(move |level| {
                    logging::install_child_logs(level, format);
                })),
            )
        }
        ChildMode::BadArguments(error) => {
            eprintln!("horsies: child mode cannot start: {error}");
            return ExitCode::from(exit_byte(ChildHostExit::BadArguments.exit_code()));
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
    run_command(app, cli.command, format)
}

/// The commands of the runner.
#[derive(Debug, Parser)]
#[command(
    about = "Run the horsies worker, the scheduler or the check of this app, or serve its fleet \
             metrics"
)]
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
        log_level: LogLevel,
        /// Serve the worker metrics (GET /metrics) and the health probes
        /// (GET /livez, GET /readyz) on HOST:PORT, or [IPV6]:PORT. No
        /// default: without it the worker serves no endpoint.
        #[arg(long, value_name = "HOST:PORT", value_parser = ListenAddress::parse)]
        metrics_listen: Option<ListenAddress>,
    },
    /// Start the scheduler of AppConfig.schedule.
    Scheduler {
        #[arg(long, value_enum, ignore_case = true, default_value = "INFO")]
        log_level: LogLevel,
    },
    /// Check the app without starting a service.
    Check {
        /// Also connect to the database and compare each worker profile
        /// with the server's connection limit.
        #[arg(long)]
        live: bool,
        #[arg(long, value_enum, ignore_case = true, default_value = "WARNING")]
        log_level: LogLevel,
    },
    /// Serve the fleet metrics of the app's database at GET /metrics.
    Metrics {
        /// The address to serve on: HOST:PORT, or [IPV6]:PORT. Required;
        /// there is no default.
        #[arg(long, value_name = "HOST:PORT", value_parser = ListenAddress::parse)]
        metrics_listen: ListenAddress,
        #[arg(long, value_enum, ignore_case = true, default_value = "INFO")]
        log_level: LogLevel,
    },
}

/// The `--log-level` values, Python's names. The shipped binary's `web`
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

/// Install the logs in `format`, then run `command`. Returns the exit
/// status.
fn run_command(mut app: Horsies, command: Command, format: LogFormat) -> ExitCode {
    match command {
        Command::Worker {
            profile,
            log_level,
            metrics_listen,
        } => {
            logging::install_logs(log_level.child_level(), format);
            let options = match profile {
                Some(name) => {
                    WorkerRunOptions::profile(name).with_log_level(log_level.child_level())
                }
                None => WorkerRunOptions::without_profile(log_level.child_level()),
            };
            let options = match metrics_listen {
                Some(address) => options.with_metrics_listen(address),
                None => options,
            };
            block_on_process_runtime(app.run_worker(options)).map_or(ExitCode::FAILURE, worker_exit)
        }
        Command::Scheduler { log_level } => {
            logging::install_logs(log_level.child_level(), format);
            app.set_role(AppRole::Scheduler);
            block_on_process_runtime(app.run_scheduler()).map_or(ExitCode::FAILURE, scheduler_exit)
        }
        Command::Check { live, log_level } => {
            logging::install_logs(log_level.child_level(), format);
            let checked = match live {
                false => app.check(),
                true => match block_on_process_runtime(app.check_live()) {
                    Some(checked) => checked,
                    None => return ExitCode::FAILURE,
                },
            };
            check_exit(checked)
        }
        Command::Metrics {
            metrics_listen,
            log_level,
        } => {
            logging::install_logs(log_level.child_level(), format);
            block_on_process_runtime(metrics::serve(&app, &metrics_listen))
                .map_or(ExitCode::FAILURE, metrics_exit)
        }
    }
}

/// Run `future` to its end on the core's process runtime from this thread.
/// `None` when the OS refused the runtime's threads; the cause is printed.
///
/// # Panics
///
/// In a process that was forked without `exec` after the core's process
/// runtime started. Nothing ran.
fn block_on_process_runtime<F: std::future::Future>(future: F) -> Option<F::Output> {
    match runtime_for(format_args!("horsies::cli::run")) {
        Ok(runtime) => Some(runtime.runtime().block_on(future)),
        Err(failed) => {
            eprintln!("horsies: {failed}");
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
            | WorkerRunError::RuntimeStartFailed(_)
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
            | SchedulerRunError::RuntimeStartFailed(_)),
        ) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}

/// The exit status of a `metrics` run, with its log line.
fn metrics_exit(end: Result<(), metrics::MetricsFailed>) -> ExitCode {
    match end {
        Ok(()) => {
            tracing::info!("fleet metrics server stopped");
            ExitCode::SUCCESS
        }
        Err(error) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}

/// The exit status of a check: the success text on stdout, or the report on
/// stderr (Python `check_command`).
fn check_exit(checked: Result<CheckPassed, ValidationReport>) -> ExitCode {
    match checked {
        Ok(passed) => {
            println!("{passed}");
            ExitCode::SUCCESS
        }
        Err(report) => print_report(&report),
    }
}

fn print_report(report: &ValidationReport) -> ExitCode {
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
        let cli = parse(&["app", "worker", "--profile", "web", "--log-level", "debug"]).unwrap();
        match cli.command {
            Command::Worker {
                profile,
                log_level,
                metrics_listen,
            } => {
                assert_eq!(profile.as_deref(), Some("web"));
                assert_eq!(log_level, LogLevel::Debug);
                assert_eq!(metrics_listen, None);
            }
            other => panic!("expected worker, got {other:?}"),
        }
    }

    #[test]
    fn worker_takes_a_metrics_listen_address() {
        for (text, host, port) in [
            ("0.0.0.0:9100", "0.0.0.0", 9100),
            ("[::1]:9100", "::1", 9100),
            ("metrics.internal:65535", "metrics.internal", 65_535),
        ] {
            let cli = parse(&[
                "app",
                "worker",
                "--profile",
                "web",
                "--metrics-listen",
                text,
            ])
            .unwrap();
            match cli.command {
                Command::Worker {
                    metrics_listen: Some(address),
                    ..
                } => {
                    assert_eq!((address.host(), address.port()), (host, port));
                }
                other => panic!("expected worker with an address, got {other:?}"),
            }
        }
    }

    #[test]
    fn worker_with_a_metrics_listen_address_that_does_not_parse_is_a_usage_error() {
        let too_long = format!("{}:9100", "a".repeat(254));
        for (address, refusal) in [
            ("9100", "is not HOST:PORT"),
            (":9100", "has no host before the port"),
            ("localhost:0", "is not a number from 1 to 65535"),
            ("localhost:65536", "is not a number from 1 to 65535"),
            ("::1:9100", "write an IPv6 address in brackets"),
            (too_long.as_str(), "has more than 253 bytes"),
        ] {
            let error = parse(&[
                "app",
                "worker",
                "--profile",
                "web",
                "--metrics-listen",
                address,
            ])
            .unwrap_err();
            assert_eq!(error.exit_code(), 2, "{address}");
            assert!(error.to_string().contains(refusal), "{address}: {error}");
        }
        let error = parse(&["app", "worker", "--profile", "web", "--metrics-listen"]).unwrap_err();
        assert_eq!(error.exit_code(), 2);
    }

    #[test]
    fn each_command_has_python_s_default_level() {
        let Command::Worker {
            profile,
            log_level,
            metrics_listen,
        } = parse(&["app", "worker"]).unwrap().command
        else {
            panic!("expected worker");
        };
        assert_eq!((profile, log_level), (None, LogLevel::Info));
        assert_eq!(metrics_listen, None);
        let Command::Check { live, log_level } = parse(&["app", "check"]).unwrap().command else {
            panic!("expected check");
        };
        assert_eq!((live, log_level), (false, LogLevel::Warning));
        assert!(matches!(
            parse(&["app", "scheduler"]).unwrap().command,
            Command::Scheduler {
                log_level: LogLevel::Info
            }
        ));
    }

    #[test]
    fn an_unknown_level_and_an_unknown_command_are_usage_errors() {
        for arguments in [
            &["app", "worker", "--log-level", "TRACE"][..],
            &["app", "worker", "--loglevel", "INFO"][..],
            &["app", "web"][..],
            &["app"][..],
        ] {
            let error = parse(arguments).unwrap_err();
            assert_eq!(error.exit_code(), 2, "{arguments:?}");
        }
    }

    #[test]
    fn metrics_takes_a_metrics_listen_address_and_python_s_default_level() {
        let cli = parse(&["app", "metrics", "--metrics-listen", "127.0.0.1:9100"]).unwrap();
        match cli.command {
            Command::Metrics {
                metrics_listen,
                log_level,
            } => {
                assert_eq!(metrics_listen.to_string(), "127.0.0.1:9100");
                assert_eq!(log_level, LogLevel::Info);
            }
            other => panic!("expected metrics, got {other:?}"),
        }
        let cli = parse(&["app", "metrics", "--metrics-listen", "[::1]:9100"]).unwrap();
        match cli.command {
            Command::Metrics { metrics_listen, .. } => assert_eq!(metrics_listen.host(), "::1"),
            other => panic!("expected metrics, got {other:?}"),
        }
    }

    #[test]
    fn metrics_without_metrics_listen_is_a_usage_error() {
        let error = parse(&["app", "metrics"]).unwrap_err();
        assert_eq!(error.exit_code(), 2);
        assert!(error.to_string().contains("--metrics-listen"), "{error}");
        let error = parse(&["app", "metrics", "--listen", "127.0.0.1:9100"]).unwrap_err();
        assert_eq!(error.exit_code(), 2);
    }

    #[test]
    fn metrics_with_an_address_that_does_not_parse_is_a_usage_error() {
        for (address, refusal) in [
            ("9100", "is not HOST:PORT"),
            (":9100", "has no host before the port"),
            ("localhost:0", "is not a number from 1 to 65535"),
            ("::1:9100", "write an IPv6 address in brackets"),
        ] {
            let error = parse(&["app", "metrics", "--metrics-listen", address]).unwrap_err();
            assert_eq!(error.exit_code(), 2, "{address}");
            assert!(error.to_string().contains(refusal), "{address}: {error}");
        }
    }

    #[test]
    fn critical_logs_at_the_child_level_critical() {
        assert_eq!(LogLevel::Critical.child_level(), WorkerLogLevel::Critical);
        assert_eq!(LogLevel::Warning.child_level(), WorkerLogLevel::Warning);
    }
}

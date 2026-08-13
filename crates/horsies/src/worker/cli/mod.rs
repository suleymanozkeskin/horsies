pub mod banner;
pub mod cutover;
pub mod transcode;
pub mod web;

use std::fmt;
use std::str::FromStr;

use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt as subscriber_fmt, EnvFilter};

/// Log level accepted by the `--loglevel` flag.
///
/// Mirrors the Python CLI choices (DEBUG, INFO, WARNING, ERROR)
/// mapped to the corresponding `tracing` levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Debug => write!(f, "debug"),
            LogLevel::Info => write!(f, "info"),
            LogLevel::Warning => write!(f, "warn"),
            LogLevel::Error => write!(f, "error"),
        }
    }
}

impl FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "debug" => Ok(LogLevel::Debug),
            "info" => Ok(LogLevel::Info),
            "warning" | "warn" => Ok(LogLevel::Warning),
            "error" => Ok(LogLevel::Error),
            "critical" => Ok(LogLevel::Error), // map CRITICAL -> error (tracing has no CRITICAL)
            other => Err(format!(
                "invalid log level '{}': expected one of debug, info, warning, error",
                other,
            )),
        }
    }
}

/// Horsies task queue worker CLI.
///
/// In Rust, tasks are registered at compile time, so there is no runtime
/// module discovery like in Python. Instead, the module/config path argument
/// specifies a configuration source (e.g. a TOML/JSON config file, or a
/// DSN string) that the binary entrypoint uses to load `AppConfig`.
///
/// # Examples
///
/// ```text
/// # Start a worker with a config file
/// horsies worker ./config/horsies.toml
///
/// # Start a worker with a database URL
/// horsies worker -m postgres://user:pass@localhost/mydb
///
/// # Validate configuration
/// horsies check ./config/horsies.toml --live
/// ```
#[derive(Debug, Parser)]
#[command(
    name = "horsies",
    about = "Horsies distributed task queue",
    after_help = "\
Examples:
  horsies worker ./config/horsies.toml
  horsies worker -m postgres://user:pass@localhost/mydb
  horsies worker ./config/horsies.toml -q high,low --concurrency 8
  horsies check ./config/horsies.toml
  horsies check ./config/horsies.toml --live"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// Available CLI commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start a worker process.
    Worker(WorkerArgs),
    /// Start the scheduler service.
    Scheduler(SchedulerArgs),
    /// Validate configuration and optionally ping the broker.
    Check(CheckArgs),
    /// Fetch documentation locally for AI agents.
    GetDocs(GetDocsArgs),
    /// Operate the offline task-history cutover.
    Cutover(cutover::CutoverArgs),
    /// Operate replacement-partition archive transcodes.
    Transcode(transcode::TranscodeArgs),
    /// Serve the monitoring web UI.
    Web(web::WebArgs),
}

/// Arguments for the `worker` subcommand.
#[derive(Debug, clap::Args)]
pub struct WorkerArgs {
    /// Configuration source: path to a config file or a database URL.
    ///
    /// This is the Rust equivalent of Python's module path argument
    /// (e.g. `myapp.horsies_config:app`). Since Rust registers tasks at
    /// compile time, this argument points to the configuration source
    /// rather than a Python module.
    #[arg(value_name = "CONFIG")]
    pub module: Option<String>,

    /// Configuration source (alternative to positional argument).
    ///
    /// Equivalent to the positional CONFIG argument. If both are
    /// provided, this flag takes precedence.
    #[arg(short = 'm', long = "module")]
    pub module_flag: Option<String>,

    /// Queues to consume from (comma-separated).
    #[arg(short, long, value_delimiter = ',', default_value = "default")]
    pub queues: Vec<String>,

    /// Maximum number of concurrent tasks.
    #[arg(short, long)]
    pub concurrency: Option<u32>,

    /// Maximum tasks claimed per queue per pass.
    #[arg(long, default_value = "2")]
    pub max_claim_batch: u32,

    /// Maximum total CLAIMED tasks for this worker (0 = auto, defaults to
    /// concurrency in hard-cap mode and concurrency + prefetch in soft-cap
    /// mode). Increase for deeper prefetch if tasks start very quickly.
    #[arg(long, default_value = "0")]
    pub max_claim_per_worker: u32,

    /// Number of queued NOTIFY messages to drain after waking up.
    /// Prevents thundering herd on burst inserts. Default 100.
    #[arg(long, default_value = "100")]
    pub coalesce_notifies: u32,

    /// Logging level (debug, info, warning, error). Case-insensitive.
    /// Overridden by the RUST_LOG environment variable when set.
    #[arg(long, default_value = "info")]
    pub loglevel: LogLevel,
}

impl WorkerArgs {
    /// Resolve the configuration source from either the `-m`/`--module` flag
    /// or the positional argument. The flag takes precedence if both are set.
    #[allow(dead_code)] // CLI command handlers not yet implemented
    pub fn config_source(&self) -> Option<&str> {
        self.module_flag.as_deref().or(self.module.as_deref())
    }
}

/// Arguments for the `scheduler` subcommand.
#[derive(Debug, clap::Args)]
pub struct SchedulerArgs {
    /// Configuration source: path to a config file or a database URL.
    #[arg(value_name = "CONFIG")]
    pub module: Option<String>,

    /// Configuration source (alternative to positional argument).
    #[arg(short = 'm', long = "module")]
    pub module_flag: Option<String>,

    /// Logging level (debug, info, warning, error). Case-insensitive.
    /// Overridden by the RUST_LOG environment variable when set.
    #[arg(long, default_value = "info")]
    pub loglevel: LogLevel,

    /// Override the schedule check interval in seconds (1–60).
    /// If not provided, uses the value from `ScheduleConfig`.
    #[arg(long)]
    pub check_interval: Option<u32>,

    /// Validate schedules and print what would run, without actually
    /// starting the scheduler loop.
    #[arg(long)]
    pub dry_run: bool,
}

impl SchedulerArgs {
    /// Resolve the configuration source from either the `-m`/`--module` flag
    /// or the positional argument. The flag takes precedence if both are set.
    #[allow(dead_code)] // CLI command handlers not yet implemented
    pub fn config_source(&self) -> Option<&str> {
        self.module_flag.as_deref().or(self.module.as_deref())
    }
}

/// Arguments for the `check` subcommand.
#[derive(Debug, clap::Args)]
pub struct CheckArgs {
    /// Configuration source: path to a config file or a database URL.
    #[arg(value_name = "CONFIG")]
    pub module: Option<String>,

    /// Configuration source (alternative to positional argument).
    #[arg(short = 'm', long = "module")]
    pub module_flag: Option<String>,

    /// Also ping the database to verify connectivity.
    #[arg(long)]
    pub live: bool,

    /// Logging level (debug, info, warning, error). Case-insensitive.
    /// Overridden by the RUST_LOG environment variable when set.
    #[arg(long, default_value = "warning")]
    pub loglevel: LogLevel,
}

impl CheckArgs {
    /// Resolve the configuration source from either the `-m`/`--module` flag
    /// or the positional argument. The flag takes precedence if both are set.
    #[allow(dead_code)] // CLI command handlers not yet implemented
    pub fn config_source(&self) -> Option<&str> {
        self.module_flag.as_deref().or(self.module.as_deref())
    }
}

/// Arguments for the `get-docs` subcommand.
#[derive(Debug, clap::Args)]
pub struct GetDocsArgs {
    /// Output directory (default: .horsies-docs).
    #[arg(long, default_value = crate::worker::docs_fetcher::DEFAULT_OUTPUT_DIR)]
    pub output: String,
}

/// Initialize the `tracing` subscriber with the given log level.
///
/// If the `RUST_LOG` environment variable is set, it takes precedence
/// over the `level` parameter, giving operators fine-grained control
/// (e.g. `RUST_LOG=horsies_worker=debug,sqlx=warn`).
///
/// This must be called **once** early in the program before any
/// `tracing` events are emitted.
pub fn init_tracing(level: LogLevel) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        // No RUST_LOG set -- use the CLI-provided level.
        EnvFilter::new(level.to_string())
    });

    subscriber_fmt()
        .with_env_filter(env_filter)
        .with_ansi(should_use_color())
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();
}

/// Decide whether to emit ANSI color in worker/CLI logs.
///
/// tracing-subscriber's fmt layer defaults `ansi` on and does no terminal
/// detection, so on a non-TTY sink (files, journald, container log drains) the
/// escape codes are emitted literally and break grep / parsers / alert regexes.
/// Resolve it explicitly (parity with horsies PR #151):
/// `FORCE_COLOR` (set, non-empty, not `0`) forces on and wins over `NO_COLOR`;
/// else `NO_COLOR` (present, any value) forces off; else follow stderr's TTY-ness.
fn should_use_color() -> bool {
    use std::io::IsTerminal;
    resolve_color(
        std::env::var("FORCE_COLOR").ok().as_deref(),
        std::env::var_os("NO_COLOR").is_some(),
        std::io::stderr().is_terminal(),
    )
}

/// Pure color decision: `FORCE_COLOR` (non-empty, not `0`) wins over `NO_COLOR`;
/// else `NO_COLOR` present forces off; else follow the TTY-ness of the sink.
fn resolve_color(force_color: Option<&str>, no_color: bool, is_tty: bool) -> bool {
    if let Some(force) = force_color {
        if !force.is_empty() && force != "0" {
            return true;
        }
    }
    if no_color {
        return false;
    }
    is_tty
}

#[cfg(test)]
mod color_tests {
    use super::resolve_color;

    #[test]
    fn force_color_on_wins_regardless_of_tty_or_no_color() {
        assert!(resolve_color(Some("1"), false, false));
        assert!(
            resolve_color(Some("1"), true, false),
            "FORCE_COLOR wins over NO_COLOR"
        );
        assert!(resolve_color(Some("yes"), true, false));
    }

    #[test]
    fn force_color_zero_or_empty_does_not_force() {
        // Falls through to NO_COLOR / TTY.
        assert!(!resolve_color(Some("0"), false, false));
        assert!(!resolve_color(Some(""), false, false));
        assert!(
            resolve_color(Some("0"), false, true),
            "falls through to TTY"
        );
    }

    #[test]
    fn no_color_forces_off_even_on_tty() {
        assert!(!resolve_color(None, true, true));
    }

    #[test]
    fn defaults_to_tty_ness() {
        assert!(resolve_color(None, false, true));
        assert!(!resolve_color(None, false, false));
    }
}

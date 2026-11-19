//! The logs of the runner: one `tracing` subscriber on stderr, at the
//! `--loglevel` of the command, or at `RUST_LOG` when it is set. The core's
//! `tracing` events reach it. A worker child installs the same subscriber
//! at the level of its init frame, the worker's `--loglevel`.
//!
//! A subscriber that the binary installed before `run` is kept.

use std::io::IsTerminal;

use horsies_core::frame_protocol::ChildLogLevel;
use tracing_subscriber::EnvFilter;

use crate::WorkerLogLevel;

/// Install the subscriber at `level` (Python `setup_logging`). A global
/// subscriber that exists already is kept.
pub(crate) fn install_logs(level: WorkerLogLevel) {
    install(filter_text(level));
}

/// The log setup of a worker child: the subscriber at the level of its init
/// frame.
pub(super) fn install_child_logs(level: ChildLogLevel) {
    install(filter_text(worker_level(level)));
}

fn install(level: &'static str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let color = resolve_color(
        std::env::var("FORCE_COLOR").ok().as_deref(),
        std::env::var_os("NO_COLOR").is_some(),
        std::io::stderr().is_terminal(),
    );
    // A subscriber that the binary installed first stays: the runner adds
    // none.
    let _kept_or_installed = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(color)
        .with_target(true)
        .try_init();
}

/// The `tracing` filter of a level. `tracing` has no level above `error`,
/// so Python's `CRITICAL` logs errors. Pure.
fn filter_text(level: WorkerLogLevel) -> &'static str {
    match level {
        WorkerLogLevel::Debug => "debug",
        WorkerLogLevel::Info => "info",
        WorkerLogLevel::Warning => "warn",
        WorkerLogLevel::Error | WorkerLogLevel::Critical => "error",
    }
}

/// The worker level of a child's init frame level. Pure.
fn worker_level(level: ChildLogLevel) -> WorkerLogLevel {
    match level {
        ChildLogLevel::Debug => WorkerLogLevel::Debug,
        ChildLogLevel::Info => WorkerLogLevel::Info,
        ChildLogLevel::Warning => WorkerLogLevel::Warning,
        ChildLogLevel::Error => WorkerLogLevel::Error,
        ChildLogLevel::Critical => WorkerLogLevel::Critical,
    }
}

/// Whether the logs carry ANSI color: `FORCE_COLOR` (set, not empty, not
/// `0`) turns it on and wins over `NO_COLOR`; else `NO_COLOR` (set, any
/// value) turns it off; else color only on a terminal. Pure.
fn resolve_color(force_color: Option<&str>, no_color: bool, is_terminal: bool) -> bool {
    match (force_color, no_color) {
        (Some(force), _) if !force.is_empty() && force != "0" => true,
        (_, true) => false,
        (_, false) => is_terminal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn force_color_wins_over_no_color_and_the_terminal() {
        assert!(resolve_color(Some("1"), true, false));
        assert!(resolve_color(Some("yes"), false, false));
    }

    #[test]
    fn an_empty_or_zero_force_color_falls_through() {
        assert!(!resolve_color(Some("0"), false, false));
        assert!(!resolve_color(Some(""), true, true));
        assert!(resolve_color(Some("0"), false, true));
    }

    #[test]
    fn no_color_turns_color_off_on_a_terminal() {
        assert!(!resolve_color(None, true, true));
        assert!(resolve_color(None, false, true));
        assert!(!resolve_color(None, false, false));
    }

    #[test]
    fn each_level_has_its_filter_and_critical_logs_errors() {
        assert_eq!(filter_text(WorkerLogLevel::Warning), "warn");
        assert_eq!(filter_text(WorkerLogLevel::Critical), "error");
        assert_eq!(
            worker_level(ChildLogLevel::Critical),
            WorkerLogLevel::Critical
        );
        assert_eq!(worker_level(ChildLogLevel::Debug), WorkerLogLevel::Debug);
    }
}

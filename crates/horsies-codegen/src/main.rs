//! `horsies-codegen`: renders the Python files that come from horsies-core.
//!
//! - `_catalog/<module>.py`: the code catalogs (`str, Enum` or `Enum`
//!   classes) of `horsies_core::catalog::CODE_CATALOGS`.
//! - `_catalog/manifest.py`: the producer manifest
//!   (`horsies_core::manifest_schema::PRODUCER_MANIFEST`) as frozen
//!   dataclasses, its integer bounds and its dump function.
//! - `_catalog/worker_manifest.py`: the worker manifest
//!   (`horsies_core::manifest_schema::WORKER_MANIFEST`) in the same form.
//! - `_catalog/scheduler_manifest.py`: the scheduler manifest
//!   (`horsies_core::manifest_schema::SCHEDULER_MANIFEST`) in the same form.
//! - `_catalog/workflow_spec.py`: the workflow spec data
//!   (`horsies_core::manifest_schema::WORKFLOW_SPEC_DATA`) in the same form.
//! - `_catalog/release_bounds.py` and `_catalog/monitoring_bounds.py`:
//!   integer constants of horsies-core (`constants::CONSTANT_MODULES`).
//! - `_catalog/broker_errors.py`, `_catalog/workflow_handle_errors.py` and
//!   `_catalog/monitoring_errors.py`: the error classes, subsets and unions
//!   of `horsies_core::error_catalog`, and `_catalog/error_detail.py` with
//!   `NoCause`.
//! - `_core.pyi`: the type stubs of the extension module `horsies._core`.
//!
//! Usage:
//!
//! ```text
//! horsies-codegen write <package-dir>   write the files
//! horsies-codegen check <package-dir>   compare; exit 1 when a file is stale
//! ```
//!
//! `<package-dir>` is the folder of the `horsies` Python package. CI runs
//! `check` and fails on a stale file.

mod catalogs;
mod constants;
mod core_stub;
mod error_classes;
mod manifest;
mod output;
mod python_text;
mod surface;

use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use output::{check_all, render_all, write_all, CheckReport};

/// The files are written, or they are current.
const EXIT_DONE: u8 = 0;
/// `check` found a stale file.
const EXIT_STALE: u8 = 1;
/// The arguments are not a command.
const EXIT_USAGE: u8 = 2;
/// A file operation failed.
const EXIT_FILE_ERROR: u8 = 3;

const USAGE: &str = "usage: horsies-codegen (write | check) <package-dir>";

/// One run of the tool.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    Write { package_dir: PathBuf },
    Check { package_dir: PathBuf },
}

/// Why the arguments are not a command.
#[derive(Debug, PartialEq, Eq)]
enum UsageError {
    MissingCommand,
    UnknownCommand(String),
    MissingPackageDir,
    ExtraArguments,
}

fn parse_arguments(arguments: Vec<OsString>) -> Result<Command, UsageError> {
    let mut arguments = arguments.into_iter();
    let command = arguments.next().ok_or(UsageError::MissingCommand)?;
    let package_dir = PathBuf::from(arguments.next().ok_or(UsageError::MissingPackageDir)?);
    if arguments.next().is_some() {
        return Err(UsageError::ExtraArguments);
    }
    match command.to_str() {
        Some("write") => Ok(Command::Write { package_dir }),
        Some("check") => Ok(Command::Check { package_dir }),
        Some(other) => Err(UsageError::UnknownCommand(other.to_owned())),
        None => Err(UsageError::UnknownCommand(
            command.to_string_lossy().into_owned(),
        )),
    }
}

fn main() -> ExitCode {
    let command = match parse_arguments(std::env::args_os().skip(1).collect()) {
        Ok(command) => command,
        Err(error) => {
            report(&format!("{error:?}\n{USAGE}"));
            return ExitCode::from(EXIT_USAGE);
        }
    };
    let files = render_all();
    match command {
        Command::Write { package_dir } => match write_all(&package_dir, &files) {
            Ok(written) => {
                let removed: Vec<String> = written
                    .removed
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect();
                report(&format!(
                    "wrote {} files under {}; removed: [{}]",
                    written.written,
                    package_dir.display(),
                    removed.join(", ")
                ));
                ExitCode::from(EXIT_DONE)
            }
            Err(error) => {
                report(&error.to_string());
                ExitCode::from(EXIT_FILE_ERROR)
            }
        },
        Command::Check { package_dir } => match check_all(&package_dir, &files) {
            Ok(CheckReport::Current) => ExitCode::from(EXIT_DONE),
            Ok(CheckReport::Stale {
                differing,
                missing,
                extra,
            }) => {
                report(&format!(
                    "generated files are stale; run `horsies-codegen write {}`\n\
                     differing: {differing:?}\nmissing: {missing:?}\nextra: {extra:?}",
                    package_dir.display()
                ));
                ExitCode::from(EXIT_STALE)
            }
            Err(error) => {
                report(&error.to_string());
                ExitCode::from(EXIT_FILE_ERROR)
            }
        },
    }
}

/// Write one message to stderr. A closed stderr is ignored: the exit code
/// carries the outcome.
fn report(message: &str) {
    let _write_failure_ignored = writeln!(std::io::stderr(), "{message}");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(words: &[&str]) -> Vec<OsString> {
        words.iter().map(OsString::from).collect()
    }

    #[test]
    fn write_and_check_take_a_package_folder() {
        assert_eq!(
            parse_arguments(arguments(&["write", "horsies"])),
            Ok(Command::Write {
                package_dir: PathBuf::from("horsies")
            })
        );
        assert_eq!(
            parse_arguments(arguments(&["check", "python/horsies"])),
            Ok(Command::Check {
                package_dir: PathBuf::from("python/horsies")
            })
        );
    }

    #[test]
    fn a_wrong_command_line_is_a_usage_error() {
        assert_eq!(
            parse_arguments(arguments(&[])),
            Err(UsageError::MissingCommand)
        );
        assert_eq!(
            parse_arguments(arguments(&["write"])),
            Err(UsageError::MissingPackageDir)
        );
        assert_eq!(
            parse_arguments(arguments(&["render", "horsies"])),
            Err(UsageError::UnknownCommand("render".to_owned()))
        );
        assert_eq!(
            parse_arguments(arguments(&["write", "horsies", "more"])),
            Err(UsageError::ExtraArguments)
        );
    }
}

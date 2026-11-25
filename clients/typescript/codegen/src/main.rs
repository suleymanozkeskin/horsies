//! `horsies-typescript-codegen`: renders the TypeScript modules of the
//! `horsies` npm package that come from horsies-core.
//!
//! The modules go to `src/_catalog/` of the package folder. Each one starts
//! with [`ts_text::GENERATED_HEADER`]. The output set is in
//! [`output::render_all`].
//!
//! Usage:
//!
//! ```text
//! horsies-typescript-codegen write <package-dir>   write the files
//! horsies-typescript-codegen check <package-dir>   compare; exit 1 when a file is stale
//! ```
//!
//! `<package-dir>` is the folder of the npm package
//! (`clients/typescript/horsies`). CI runs `check` and fails on a stale file.

mod catalogs;
mod error_catalogs;
mod error_detail;
mod manifest;
mod model;
mod output;
mod retention_class;
mod task_code;
mod ts_text;
mod wire_json;

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

const USAGE: &str = "usage: horsies-typescript-codegen (write | check) <package-dir>";

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

/// The command of `arguments`, the program name left out. Pure.
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
                    "generated files are stale; run \
                     `cargo run -p horsies-typescript-codegen -- write {}`\n\
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
    fn write_takes_a_package_folder() {
        assert_eq!(
            parse_arguments(arguments(&["write", "clients/typescript/horsies"])),
            Ok(Command::Write {
                package_dir: PathBuf::from("clients/typescript/horsies")
            })
        );
    }

    #[test]
    fn check_takes_a_package_folder() {
        assert_eq!(
            parse_arguments(arguments(&["check", "clients/typescript/horsies"])),
            Ok(Command::Check {
                package_dir: PathBuf::from("clients/typescript/horsies")
            })
        );
    }

    #[test]
    fn no_arguments_is_a_missing_command() {
        assert_eq!(
            parse_arguments(arguments(&[])),
            Err(UsageError::MissingCommand)
        );
    }

    #[test]
    fn a_command_without_a_folder_is_a_missing_folder() {
        assert_eq!(
            parse_arguments(arguments(&["write"])),
            Err(UsageError::MissingPackageDir)
        );
    }

    #[test]
    fn another_word_is_an_unknown_command() {
        assert_eq!(
            parse_arguments(arguments(&["render", "horsies"])),
            Err(UsageError::UnknownCommand("render".to_owned()))
        );
    }

    #[test]
    fn a_third_argument_is_refused() {
        assert_eq!(
            parse_arguments(arguments(&["write", "horsies", "more"])),
            Err(UsageError::ExtraArguments)
        );
    }
}

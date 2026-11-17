//! The commands of the shipped `horsies` binary: `web`, `get-docs` and
//! `skills install`. Not part of the library API: the binary's `main` calls
//! [`run`].
//!
//! The worker, the scheduler and the check of an app run from the app's own
//! binary through `horsies::cli::run(app)`, because the app's tasks are
//! compiled into it. This binary refuses `worker`, `scheduler` and `check` by
//! name and says where they run.
//!
//! Exit codes: 0 when a command succeeds, and when an operator stop
//! (`SIGTERM`, `SIGINT`) ends `web`; 1 when a command fails; 2 for arguments
//! that do not parse, a command of the app's binary, and the arguments that
//! `web` refuses before it starts (as argparse and clap give it).

mod docs;
mod skills;
mod web;

use std::ffi::OsString;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

/// The exit status of an argument error, as argparse and clap give it.
const USAGE_EXIT: u8 = 2;

/// The commands of the app's own binary (`horsies::cli::run`).
const APP_BINARY_COMMANDS: [&str; 3] = ["worker", "scheduler", "check"];

/// The text after the command list of `horsies --help`.
const AFTER_HELP: &str = "\
The worker, the scheduler and the check of an app run from the app's own binary, which
has the app's tasks compiled in. Call horsies::cli::run(app) in its main, then run:
  <your binary> worker --profile <name>
  <your binary> scheduler
  <your binary> check [--live]

Examples:
  horsies web horsies.toml
  horsies web --database-url postgresql://user:pw@localhost/db
  horsies get-docs
  horsies skills install --target .claude/skills";

/// The commands of the shipped binary.
#[derive(Debug, Parser)]
#[command(
    name = "horsies",
    version,
    about = "The horsies tools of the Rust client: the monitoring web UI, the docs and the agent skill",
    after_help = AFTER_HELP
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the monitoring web UI of a database.
    Web(web::WebArgs),
    /// Fetch the docs of this release for AI agents: the shared pages and the
    /// Rust pages.
    GetDocs(docs::GetDocsArgs),
    /// Manage the horsies agent skill folder.
    Skills {
        #[command(subcommand)]
        command: SkillsCommand,
    },
}

#[derive(Debug, Subcommand)]
enum SkillsCommand {
    /// Copy the skill folder of this release to <target>/horsies/.
    Install(skills::InstallArgs),
}

/// Run the command of this process's arguments. Blocks the calling thread
/// until the command ends; call it from a plain `fn main`.
pub fn run() -> ExitCode {
    let arguments: Vec<OsString> = std::env::args_os().collect();
    if let Some(refusal) = app_binary_command_refusal(&arguments) {
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
    match cli.command {
        Command::Web(args) => web::run(&args),
        Command::GetDocs(args) => docs::run(&args),
        Command::Skills {
            command: SkillsCommand::Install(args),
        } => skills::run(&args),
    }
}

/// The refusal of a command of the app's own binary given as the command of
/// this binary; `None` for any other first argument. Pure.
fn app_binary_command_refusal(arguments: &[OsString]) -> Option<String> {
    let command = arguments.get(1)?.to_str()?;
    APP_BINARY_COMMANDS.contains(&command).then(|| {
        format!(
            "`horsies {command}` is not a command of this binary: the {command} of an app runs \
             from the app's own binary, which has the app's tasks compiled in. Call \
             horsies::cli::run(app) in its main and run `<your binary> {command}`."
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn the_commands_of_the_app_binary_are_refused_by_name() {
        for command in APP_BINARY_COMMANDS {
            let refusal = app_binary_command_refusal(&arguments(&["horsies", command]))
                .expect("a command of the app binary is refused");
            assert!(refusal.contains("horsies::cli::run(app)"), "{refusal}");
            assert!(
                refusal.contains(&format!("`<your binary> {command}`")),
                "{refusal}"
            );
        }
    }

    #[test]
    fn the_commands_of_this_binary_are_not_refused() {
        for command in ["web", "get-docs", "skills", "--help"] {
            assert_eq!(
                app_binary_command_refusal(&arguments(&["horsies", command])),
                None
            );
        }
        assert_eq!(app_binary_command_refusal(&arguments(&["horsies"])), None);
    }

    #[test]
    fn the_help_names_the_runner_and_no_removed_command() {
        let help = <Cli as clap::CommandFactory>::command()
            .render_long_help()
            .to_string();
        assert!(help.contains("horsies::cli::run(app)"), "{help}");
        for removed in ["cutover", "transcode"] {
            assert!(!help.contains(removed), "{help}");
        }
        let names: Vec<String> = <Cli as clap::CommandFactory>::command()
            .get_subcommands()
            .map(|command| command.get_name().to_owned())
            .collect();
        assert_eq!(names, ["web", "get-docs", "skills"]);
    }

    #[test]
    fn skills_install_needs_a_target() {
        let error = Cli::try_parse_from(["horsies", "skills", "install"])
            .expect_err("the target is required");
        assert_eq!(error.exit_code(), 2);
        let parsed = Cli::try_parse_from(["horsies", "skills", "install", "--target", "x"]);
        assert!(parsed.is_ok());
    }
}

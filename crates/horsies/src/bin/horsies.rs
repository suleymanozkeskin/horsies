use std::process::ExitCode;

use clap::Parser;
use horsies::{execute_cutover, execute_transcode, Cli, Command};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Cutover(args) => match execute_cutover(args).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("cutover failed: {error}");
                ExitCode::FAILURE
            }
        },
        Command::Transcode(args) => match execute_transcode(args).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("transcode failed: {error}");
                ExitCode::FAILURE
            }
        },
        Command::GetDocs(args) => match horsies::fetch_docs(&args.output) {
            Ok(count) => {
                println!("fetched {count} documentation files");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("documentation fetch failed: {error}");
                ExitCode::FAILURE
            }
        },
        Command::Worker(_) | Command::Scheduler(_) | Command::Check(_) => {
            eprintln!(
                "worker, scheduler, and application checks require an application-specific \
                 binary with its task registry linked; use that binary's command surface"
            );
            ExitCode::FAILURE
        }
    }
}

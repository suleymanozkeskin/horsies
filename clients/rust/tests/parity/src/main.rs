//! The Rust scenario binary of the parity runs.
//!
//! - `horsies-parity send <scenario>`: send the scenario on the database of
//!   `HORSIES_PARITY_DATABASE_URL`, rerun its reruns, start its workflows,
//!   run its actions and ping, and print one JSON line per send, rerun,
//!   workflow start and ping (`crossclient/parity/send.py` prints the same lines).
//!   Exit 0, 1 on a refused step, 2 on bad arguments.
//! - any other command goes to `horsies::cli::run`: `worker --profile <name>`
//!   and the worker's children, and `scheduler`.

use std::process::ExitCode;

use horsies_parity_tests::{build_app, run_sends, scenario_named, DATABASE_URL_ENV};

fn main() -> ExitCode {
    let Some(database_url) = std::env::var(DATABASE_URL_ENV)
        .ok()
        .filter(|url| !url.is_empty())
    else {
        eprintln!("horsies-parity: {DATABASE_URL_ENV} is not set");
        return ExitCode::FAILURE;
    };
    let parity = match build_app(&database_url) {
        Ok(built) => built,
        Err(error) => {
            eprintln!("horsies-parity: {error}");
            return ExitCode::FAILURE;
        }
    };
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match arguments.as_slice() {
        [command, scenario] if command == "send" => {
            let Some(scenario) = scenario_named(scenario) else {
                eprintln!("horsies-parity: no scenario {scenario}");
                return ExitCode::from(2);
            };
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!("horsies-parity: the send runtime did not start: {error}");
                    return ExitCode::FAILURE;
                }
            };
            match runtime.block_on(run_sends(&parity, scenario)) {
                Ok(lines) => {
                    for line in lines {
                        println!("{line}");
                    }
                    ExitCode::SUCCESS
                }
                Err(refused) => {
                    eprintln!("horsies-parity: {}", refused.0);
                    ExitCode::FAILURE
                }
            }
        }
        _ => horsies::cli::run(parity.app),
    }
}
